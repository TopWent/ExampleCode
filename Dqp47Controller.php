<?php
/**
 * Metric 12.5 + 12.6 Поиск КД с разными суммами в КИ и ИЧ
 *
 * Запуск
 * ./yii dq --task=Dqp47 prefix=clif
 */

namespace console\controllers\tasks\select\regular;

use console\controllers\FileTaskController;
use console\exceptions\DqException;
use console\helpers\DataHelpers\BaseDataHelper;
use console\helpers\DataHelpers\ContractHelper;
use console\models\DumpContract;
use console\helpers\DataHelpers\FipCreditHelper;
use console\helpers\DataHelpers\FipCreditHistHelper;
use console\helpers\DataHelpers\FtalHelper;
use ReportData\ContractSourceCouchBaseWithoutUnits;
use console\helpers\GlobalHelper;
use ReportData\description\ContractData;
use ReportData\description\Tr;
use yii\base\NotSupportedException;
use yii\db\Exception;

class Dqp47Controller extends FileTaskController
{
    /**
     * Транзакция не найдена
     */
    const MSG_TXT = 'Транзакция не найдена ';

    /**
     * состояния договора
     */
    const BAD_STATUS_ACTIVE_CREDIT = [9,20];

    /** Статус 7 - кредит отменен */
    const CREDIT_CANCEL = 7;

    /**
     * @var bool
     */
    protected $useFullNameAsFilename = true;

    /**
     * @var bool
     */
    protected $check_encoding = false;

    /**
     * @var bool
     */
    protected $saveLastPosition = false;

    /**
     * Получаем файл и задаем prefix
     * @param $action
     * @return bool
     * @throws DqException
     * @throws NotSupportedException
     */
    public function beforeAction($action)
    {
        /** Шапка результирующего списка */
        $this->successLog('id_partner;contract_no;contract_date;type_credit;sum;partner_id;cred_no;cred_date;hist_cred_type;cred_sum;hist_cred_sum');

        GlobalHelper::$prefix = 'clif';
        $this->filename = DumpContractsController::getFilePath();

        return parent::beforeAction($action);
    }

    /**
     * Gроверка статуса транзакции
     * @param $activeCredit - active_credit not in (9,20) or active_credit is null
     * @return bool - false(хорошо)/true(плохо)
     */
    protected function contractIsActive($activeCredit)
    {
        return (in_array($activeCredit, static::BAD_STATUS_ACTIVE_CREDIT));
    }

    /**
     * Получаем последнюю транзакцию и проверяем её
     * @param $contractData
     * @return Tr|null
     */
    protected function getLastHistData(ContractSourceCouchBaseWithoutUnits $contractData){
        $contract = $contractData->getContract();
        $lastHist = $contractData->LastTr ?? null;

        if (!$lastHist) {
            $error['id'] = $contract->getCredId();
            $error['msg'] = static::MSG_TXT;
            $this->customLog($error, 'error');
            return null;
        }

        if ($this->contractIsActive($contract->getCredActive())) {
            return null;
        }
        
        return $lastHist;
    }

    /**
     * Собираем данные из дампа и коучбейс
     * @param $row
     * @return bool
     * @throws \Exception
     */
    protected function doJob($row)
    {
        $contract = new DumpContract($row);
        if ($contract) {
            if (
                $contract->getCredId()
                && $contract->getTypeCredit() != ContractHelper::SEARCH_NOT_GUARANTEE
                && $contract->getActiveCredit() != static::CREDIT_CANCEL
            ) {
                GlobalHelper::$partnerId = $contract->getIdPartner();
                if (!$this->isAlivePartner(GlobalHelper::$partnerId)) {
                    return false;
                }
                /** Получаем договор из коуча */
                $dataContract = $this->getContractFormCouch($contract->getCredId());
                if (!$dataContract) {
                    return false;
                }
                $lastHist = $this->getLastHistData($dataContract);
                if (!$lastHist) {
                    return false;
                }

                $contractCouch = $dataContract->getContract();

                /** Ищем в ИЧ аналогичный договор (по совпадению contract_no, id_partner, contract_date, type_credit) */
                if (!($fips = $this->searchFip($contractCouch))) {
                    return false;
                }

                foreach ($fips as $fip) {
                    /** Ищем различающиеся суммы */
                    if ($fip['hist_cred_sum'] != $lastHist->Sum) {
                        /** Формирование лога */
                        $this->successLog(
                            $contractCouch->getTriggersData()->Pid . ';' .
                            $contractCouch->getContractNo() . ';' .
                            $contractCouch->CredDate . ';' .
                            $contractCouch->getCredType() . ';' .
                            $dataContract->LastTr->getSum() . ';' .
                            $fip['partner_id'] . ';' .
                            $fip['cred_no'] . ';' .
                            $fip['cred_date'] . ';' .
//                    $fip['cred_type'] . ';' .
                            $fip['hist_cred_type'] . ';' .
                            $fip['cred_sum'] . ';' .
                            $fip['hist_cred_sum']
                        );
                    }
                }
            }
        }
        return true;
    }

    /**
     * Поиск ИЧ договора (по совпадению contract_no, id_partner, contract_date, type_credit)
     *
     * @param ContractData $contractCouch
     * @return array
     * @throws Exception
     */
    private function searchFip($contractCouch)
    {
        /** Ищем ИЧ договоры (по совпадению contract_no, id_partner, contract_date, type_credit) */
        $fipCredits = FipCreditHelper::searchAllByNoAndParams(
            $contractCouch->getContractNo(), [
                'cred_date'  => $contractCouch->CredDate,
                'cred_type'  => $contractCouch->getCredType() == 90
                    ? BaseDataHelper::CRED_TYPE_GUARANTEE
                    : BaseDataHelper::SEARCH_NOT_GUARANTEE,
        ]);

        if (!$fipCredits)
        {
            return [];
        }

        /** Проверка по всем найденным КЧ */
        foreach ($fipCredits as $k => $fipCredit)
        {
            /** Получаем актуальную запись по ИЧ (fake = 0) */
            $ftal = FtalHelper::searchByCreditIdAndParams(
                $fipCredit['id'],
                ['credit_id_hist'=> ['type' => '>', 'value' => '0']]
            );

            if (!$ftal)
            {
                unset($fipCredits[$k]);
                continue;
            }

            /** Получаем транзакцию из КЧ ФИЧ */
            $credHist = FipCreditHistHelper::selectOneById($ftal['credit_id_hist']);
            if ($credHist['cred_sum'] == 0)
            {
                unset($fipCredits[$k]);
                continue;
            }

            /** Сумму и тип кредита берем из актуальной транзакции */
            $fipCredits[$k]['hist_cred_sum'] = $credHist['cred_sum'];
            $fipCredits[$k]['hist_cred_type'] = $credHist['cred_type'];
        }

        return $fipCredits;
    }
}