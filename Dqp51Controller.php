<?php
/**
 * Метрика 5.12
 * Поиск в КИ дублей среди договоров поручительства из-за смены номеров (и дат) при
 * переходе из TUTDF в XML с последующим их объединением.
 */

namespace console\controllers\tasks\select\regular;


use console\controllers\FileTaskController;
use console\exceptions\DqException;
use console\helpers\DataHelpers\BaseDataHelper;
use console\helpers\DataHelpers\ContractHelper;
use console\helpers\DataHelpers\ContractHistExtHelper;
use console\helpers\DataHelpers\ContractHistHelper;
use console\helpers\DataHelpers\ContractJointHelper;
use console\helpers\DataHelpers\FipCreditHelper;
use console\helpers\DataHelpers\FtalHelper;
use console\helpers\DataHelpers\PartnerHelper;
use console\helpers\GlobalHelper;
use console\models\DumpContract;
use ReportData\ContractSourceCouchBaseWithoutUnits;
use ReportData\description\ContractData;
use yii\base\Action;
use yii\base\NotSupportedException;
use Yii\db\Exception as DBException;

class Dqp51Controller extends FileTaskController
{
    /**
     * Заголовок CSV-файла для ФКИ.
     *
     * @const string[]
     */
    const CSV_CHF_HEADER_FIELDS = [
        'id_partner',
        'clix_id',
        'id',
        'contract_no',
        'contract_date',
        'finish_date',
        'sum',
        'currency',
        'type_credit',
        'transaction_id',
        'parent_cred_no',
        'parent_cred_date',
    ];

    /**
     * Заголовок CSV-файла для ИЧ.
     *
     * @const string[]
     */
    const CSV_IP_HEADER_FIELDS = [
        'partner_id',
        'id',
        'cred_no',
        'cred_date',
        'cred_enddate',
        'cred_sum',
        'cred_type',
        'clif_id',
    ];

    /**
     * Префиксы для заголовков CSV-файлов.
     *
     * @var array
     */
    const CSV_HEADER_PREFIXES = [
        'first_contract',
        'second_contract'
    ];

    /**
     * Идентификатор ФКИ.
     *
     * @var string
     */
    const CREDIT_HISTORY_FORMAT = 'FKI';

    /**
     * Идентификатор ИЧ.
     */
    const INFORMATION_PART = 'FIP';

    /**
     * Префикс для данных физ. лиц.
     *
     * @var string
     */
    const INDIVIDUALS = 'clif';

    /**
     * Префикс для данных юр. лиц.
     *
     * @var string
     */
    const LEGAL_ENTITIES = 'cliu';

    /**
     * Дата по умолчанию.
     *
     * @var string
     */
    const PLACEHOLDER_DATE = '1900-02-01';

    /**
     * Массив идентификаторов партнёров для создания задач.
     *
     * @var array
     */
    private $partnerIds = [];

    /**
     * @var bool
     */
    protected $useFullNameAsFilename = true;

    /**
     * @var string
     */
    protected $prefix = '';

    /**
     * @var bool
     */
    protected $check_encoding = false;

    /**
     * @var bool
     */
    protected $saveLastPosition = false;

    /**
     * @var int Флаг наличия заголовка во входном файле
     */
    protected $inFileHeader = 1;

    /**
     * @param $action
     * @return bool
     * @throws \Exception
     */
    public function beforeAction($action)
    {
        $this->filename = DumpContractsController::getFilePath();

        if (!parent::beforeAction($action)) {
            return false;
        }

        GlobalHelper::$prefix = $this->prefix;
        if ($this->prefix !== self::INDIVIDUALS && $this->prefix !== self::LEGAL_ENTITIES) {
            throw new DqException("Incorrect prefix set: $this->prefix. Possible prefixes: 'clif', 'cliu'.");
        }

        $this->partnerIds = array_column(PartnerHelper::getAllNotAgents('alive'), 'id');
        GlobalHelper::$partnerId = array_shift($this->partnerIds);



        return true;
    }

    /**
     * Мапим необходиммый массив для мержа, т.к. раньше получали массив с вытяжки из МуСкула
     * Теперь тянем данные из коуча и имеем объект, который нужно подготовить превратив в нужный массив
     *
     * @param ContractData                        $contract
     * @param ContractSourceCouchBaseWithoutUnits $dataContract
     *
     * @return array
     */
    public function prepareContract(ContractData $contract, ContractSourceCouchBaseWithoutUnits $dataContract) {
        return [
            'id_partner' => GlobalHelper::$partnerId,
            'clix_id' => $contract->getTriggersData()->getUid(),
            'id' => $contract->getCredId(),
            'contract_no' => $contract->getContractNo(),
            'contract_date' =>$contract->getCredDate(),
            'finish_date' => $contract->getCredEndDate(),
            'sum' => $contract->getCredSum(),
            'currency' => $contract->getCredCurrency(),
            'type_credit' => $contract->getCredType(),
            'transaction_id' => $dataContract->LastTr->Id,
        ];
    }

    /**
     * @param $row
     * @return bool|void
     * @throws DBException
     */
    public function doJob($row)
    {
        $contractDump = new DumpContract($row);

        GlobalHelper::$partnerId = $contractDump->getIdPartner();
        /** get contract from couch */
        $dataContract = $this->getContractFormCouch($contractDump->getCredId());

        if ($dataContract === null) {
            return;
        }
        $contract = $dataContract->getContract();

        if ($contract->getCredType() != BaseDataHelper::CRED_TYPE_GUARANTEE) {
            return;
        }

        // Получаем номер родительского договора и сравниваем с собственным номером
        $contractHistInformation = $this->getHistInformationByContractId((int)$contract->getCredId());
        // Если родительского договора нет, то можно идти дальше
        if (
            !$contractHistInformation ||
            !strlen($contractHistInformation['parent_cred_no']) ||
            !strlen($contractHistInformation['parent_cred_date'])
        ) {
            return;
        }

        if ($contractHistInformation['parent_cred_no'] === $contract->getContractNo()) {
            return;
        }

        // Получаем договор-дубликат
        $parentContract = $this->getParentContract($contract, $contractHistInformation['parent_cred_no']);
        if (!$parentContract) {
            return;
        }
        // Ищем общего заёмщика у этих договоров (достаточно одного), если он не найден - обрабатываем следующий
        $matchedContractJoint = $this->getMatchingJoint($contract, $parentContract);
        if (!$matchedContractJoint) {
            return;
        }

        // Дубликат найден; теперь, для занесения пары договоров в файл с результатами (по ФКИ), нужно получить
        // данные родительского договора (актуальную транзакцию и информацию о его родительском договоре)
        $parentContractHistInformation = $this->getHistInformationByContractId($parentContract['id']);

        $contractArr = $this->prepareContract($contract, $dataContract);

        // Маппим пару и заносим в файл ФКИ
        $contractPairCreditHistoryFormat = $this->mapPairByFields(
            array_merge($parentContract, $parentContractHistInformation),
            array_merge($contractArr, $contractHistInformation),
            self::CREDIT_HISTORY_FORMAT,
            ['clix_id' => $matchedContractJoint]
        );
        $this->addRowToResultFile($contractPairCreditHistoryFormat, self::CREDIT_HISTORY_FORMAT);

        // Для юридических лиц ИЧ не требуются
        if (GlobalHelper::$prefix === 'cliu') {
            return;
        }

        // Получаем не отменённые кредитные части в ИЧ
        $parentContractFipData = $this->getFipContractPairData($parentContract);
        if (empty($parentContractFipData)) {
            return;
        }

        $contractFipData = $this->getFipContractPairData($contractArr);

        // Если они есть для обоих договоров, заносим пару в файл с дублями
        if (!empty($contractFipData)) {
            $contractPairInformationPart = $this->mapPairByFields(
                $parentContractFipData,
                $contractFipData,
                self::INFORMATION_PART
            );
            $this->addRowToResultFile($contractPairInformationPart, self::INFORMATION_PART);
        }
    }

    /**
     * Получение данных родительского договора по ID договора.
     *
     * @param int $contractId
     * @return array
     * @throws DBException
     */
    private function getHistInformationByContractId(int $contractId): array
    {
        $result = [
            'transaction_id' => null,
            'parent_cred_no' => null,
            'parent_cred_date' => null,
        ];

        $actualTransaction = ContractHistHelper::searchByContractIdAndParams($contractId);
        if (!$actualTransaction) {
            return $result;
        }

        $extendedHistory = ContractHistExtHelper::searchAllByHistIdAndParams($actualTransaction['id']);
        if (!$extendedHistory) {
            return $result;
        }

        return [
            'transaction_id' => $actualTransaction['id'],
            'parent_cred_no' => $extendedHistory[0]['parent_cred_no'],
            'parent_cred_date' => $extendedHistory[0]['parent_cred_date']
        ];
    }

    /**
     * Получение родительского договора поручительства по договору и номеру.
     *
     * @param ContractData $childContract
     * @param string $parentCredNo
     * @return array
     * @throws DBException
     */
    private function getParentContract(ContractData $childContract, string $parentCredNo): array
    {
        // Получаем договор-дубликат, если он не найден - обрабатываем следующий
        // Массив условий индикации дубликации договоров; дата используется, если она не равна дате-заглушке
        $conditions = [
            'type_credit' => BaseDataHelper::CRED_TYPE_GUARANTEE, // Все договоры в выборке - тип 90 (поручительство)
        ];
        if ($childContract->getCredDate() !== self::PLACEHOLDER_DATE) {
            $conditions['contract_date'] = $childContract->getCredDate();
        }

        $parentContract = ContractHelper::searchAllByNoAndParams($parentCredNo, $conditions);
        return $parentContract[0] ?? [];
    }

    /**
     * Проверка на наличие общих заёмщиков по clix_contract_joint.clix_id и получение clix_id общего при наличии.
     *
     * @param ContractData $firstContract
     * @param array $secondContract
     * @return int
     * @throws DBException
     */
    private function getMatchingJoint(ContractData $firstContract, array &$secondContract): int
    {
        $firstContractJointsClixIds = array_column(
            ContractJointHelper::searchByContractIdAndParams($firstContract->getCredId()), "{$this->prefix}_id"
        );
        $secondContractJointsClixIds = array_column(
            ContractJointHelper::searchByContractIdAndParams($secondContract['id']), "{$this->prefix}_id"
        );

        $commonJointsClixIds = array_intersect($firstContractJointsClixIds, $secondContractJointsClixIds);
        return $commonJointsClixIds[0] ?? 0;
    }

    /**
     * Получение кредитной части ИЧ, связанной с договором.
     *
     * @param array $contract
     * @return array
     * @throws DBException
     */
    private function getFipContractPairData(array $contract): array
    {
        $result = [];

        $fipCredit = FipCreditHelper::searchAllByNoAndParams($contract['contract_no'], [
            'cred_date' => $contract['contract_date'],
            'cred_type' => $contract['type_credit'],
        ]);

        // Если не найдена информационная часть, дальше искать нет смысла
        if (empty($fipCredit)) {
            return $result;
        }
        $fipCredit = $fipCredit[0];

        // Получаем ID заёмщика из титульной части, если она не найдена - отменяем поиск
        $firstFipCreditApplicationLink = FtalHelper::searchByCreditIdAndParams($fipCredit['id']);
        if (empty($firstFipCreditApplicationLink)) {
            return $result;
        }

        $result[] = array_merge($fipCredit, $firstFipCreditApplicationLink);
        return $result;
    }

    /**
     * Сборка названий полей для CSV-файлов.
     *
     * @param string $postfix
     * @return array
     */
    private function getFieldNamesForCsv(string $postfix): array
    {
        $result = [];

        $fields = ($postfix == self::CREDIT_HISTORY_FORMAT)
            ? self::CSV_CHF_HEADER_FIELDS
            : self::CSV_IP_HEADER_FIELDS;

        foreach (self::CSV_HEADER_PREFIXES as $order) {
            foreach ($fields as $field) {
                $result[] = "{$order}_{$field}";
            }
        }

        return $result;
    }

    /**
     * Маппинг пары договоров для внесения в файл результатов.
     *
     * @param array $first
     * @param array $second
     * @param string $postfix
     * @param array $extraFields
     * @return array
     */
    private function mapPairByFields(array $first, array $second, string $postfix, array $extraFields = []): array
    {
        $result = [];

        $contracts = ['first_contract' => $first, 'second_contract' => $second];
        foreach ($contracts as $order => $contract) {
            if (!empty($extraFields)) {
                $contracts[$order] = array_merge($contract, $extraFields);
            }
        }

        $csvFields = $this->getFieldNamesForCsv($postfix);
        foreach ($csvFields as $csvField) {
            // Получаем название поля из $csvField в паре договоров
            $csvFieldParts = explode('_', $csvField);
            // Получаем порядок поля и его название из полного имени
            // Это сделано для сохранения минимальной цикломатической сложности
            $order = "{$csvFieldParts[0]}_{$csvFieldParts[1]}";
            $field = str_replace("{$order}_", '', $csvField);
            if ($contracts[$order][$field] ?? false) {
                $result[$csvField] = $contracts[$order][$field];
            } elseif ($contracts[$order][0][$field] ?? false) {
                $result[$csvField] = $contracts[$order][0][$field];
            }
        }

        return $result;
    }

    /**
     * Заполнение CSV-файла.
     *
     * @param array $csvReadyFields
     * @param string $postfix
     * @return bool
     */
    private function addRowToResultFile(array &$csvReadyFields, string $postfix): bool
    {
        $filename = "{$this->prefix}_{$postfix}.csv";

        $row = [];
        foreach ($csvReadyFields as $field => $value) {
            $row[$field] = $value;
        }
        $this->writeOutFileArray($row, $filename);

        return true;
    }
}
