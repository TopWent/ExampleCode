<?php
/**
 * Сформировать список всех КД (не микрозаймов), полная стоимость которых больше 1000. метрика 3,21
 *
 * Запуск
 * ./yii dq --task=dqp24
 */

namespace console\controllers\tasks\select\regular;

use console\controllers\FileTaskController;
use console\exceptions\DqException;
use console\helpers\DataHelpers\ContractHistExtHelper;
use console\helpers\DataHelpers\ContractHistHelper;
use console\helpers\GlobalHelper;
use console\helpers\ZipHelper;
use console\models\DumpContract;
use ReportData\description\ContractData;
use yii\base\Action;
use yii\base\NotSupportedException;
use yii\db\Exception;

/**
 * Class Dqp24Controller
 *
 * @package console\controllers\tasks\select\regular
 */
class Dqp24Controller extends FileTaskController

{
    /**
     * Тип кредита Микрозайм
     */
    const TYPE_CREDIT = 19;

    /**
     * Полная стоимость кредита (ПСК)
     */
    const FULL_COST = 1000;

    /**
     * @var int $lastId
     */
    protected $lastId = 0;

    /**
     * @var bool
     */
    protected $useFullNameAsFilename = true;

    /**
     * @var int Флаг наличия заголовка во входном файле
     */
    protected $inFileHeader = 1;

    /**
     * @var bool
     */
    protected $check_encoding = false;

    /**
     * @var bool
     */
    protected $saveLastPosition = false;

    /**
     * заголовок файла
     */
    const OUT_FILE_HEADER = [
        'id_partner',
        'id',
        'contract_no',
        'contract_date',
        'sum',
        'type_credit',
        'max_update',
        'update',
        'full_cost',
    ];

    /**
     * @var string Текущая дата - 60 дней
     */
    protected $dateFrom;

    /**
     * поиск последней транзакции, у которой full_cost > 1000
     * @param array $transacts
     * @return array
     * @throws Exception
     */
    protected function searchLastFullCost(array &$transacts) : array {
        $amountIndex = count($transacts);
        $amountIndex--;
        for($i=$amountIndex;$i>=0;$i--){
            $ext = ContractHistExtHelper::searchAllByHistIdAndParams(
                $transacts[$i]['id'],
                [
                    'full_cost'=>['type' => '>', 'value' => self::FULL_COST]
                ]
            );

            if($ext){
                $ext = array_shift($ext);
                $transacts[$i]['full_cost'] = $ext['full_cost'];
                return $transacts[$i];
            }

        }
        return [];
    }

    /**
     * экспорт результата
     *
     * @param ContractData $dataContract
     * @param array $lastHist
     * @param array $fullCostHist
     */
    protected function exportResult(ContractData $dataContract, array $lastHist, array $fullCostHist){
        $this->writeOutFileArray([
            $dataContract->getTriggersData()->getPid(),
            $dataContract->getCredId(),
            $dataContract->getContractNo(),
            $dataContract->getCredDate(),
            $dataContract->getCredSum(),
            $dataContract->getCredType(),
            date('d.m.Y',strtotime($lastHist['update'])),
            date('d.m.Y',strtotime($fullCostHist['update'])),
            $fullCostHist['full_cost']
        ]);
    }

    /**
     * Получаем транзации по договору выгруженных позже 60 дней назад
     * ищем транзакции, у которых full_cost > 1000
     * и пишем в файл
     *
     * @param $contract
     * @return void
     * @throws Exception
     */
    protected function doJob($contract)
    {
        $contract = new DumpContract($contract);
        if (!$contract) {
            return;
        }
        /**
         * type_credit <> '19'
         */
        if (!$contract->getCredId() ||
            $contract->getTypeCredit() == static::TYPE_CREDIT) {
            return;
        }
        /** Получаем договор из коуча */
        $couchContract = $this->getContractFormCouch($contract->getCredId());
        if (!$couchContract) {
            return;
        }
        $dataContract = $couchContract->getContract();
        /**
         * получаем хронологический массив транзакций по договору, выгруженных позже 60 дней назад
         */
        $transactions = ContractHistHelper::searchByContractIdAndParams(
            $contract->getCredId(),
            ['update' => ['type' => '>=', 'value' => $this->dateFrom]],
            false
        );
        if (empty($transactions)) {
            return;
        }
        /**
         * ищем транзакции, у которых full_cost > 1000
         */
        $fullCostHist = $this->searchLastFullCost($transactions);
        if (empty($fullCostHist)) {
            return;
        }
        /**
         * выгружаем результат
         */
        $this->exportResult(
            $dataContract,
            array_pop($transactions),
            $fullCostHist
        );

    }

    /**
     * @param $action
     *
     * @return bool
     * @throws DqException
     * @throws NotSupportedException
     */
    public function beforeAction($action)
    {
        GlobalHelper::$prefix = 'clif';
        $this->dateFrom = date('Y-m-d', strtotime('- 60 days'));
        $this->filename = DumpContractsController::getFilePath();

        if (!parent::beforeAction($action)) {
            return false;
        }

        return true;
    }

    /**
     * @param Action $action
     * @param mixed  $result
     *
     * @return mixed
     */
    public function afterAction($action, $result)
    {
        $path = $this->getTaskFolderResultPath();
        $filename = $this->getTaskFolderPath() . '/' .
            date('Y-m-d_H-i', YII_BEGIN_TIME) . '_' . ($this->setNameArch ?? 'out') . '_.zip';
        ZipHelper::zipDir($path, $filename);
        return parent::afterAction($action, $result);
    }
}