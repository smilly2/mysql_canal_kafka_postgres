package cn.com.smilly;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class PostgresSink {
    private final static ObjectMapper mapper = new ObjectMapper();;
    private static volatile long checkPointTime = 0;
    private final static Map<String,String> offset = new ConcurrentHashMap<>();
    private final static Map<String, HashSet<String>> syncTables = new HashMap();
    static {
        syncTables.put("uc", new HashSet<>(Arrays.asList("logs_update_userinfo","hnp_cardbase","uc_bank_config","uc_bank_protocol","uc_bank_card","uc_account_open","uc_basic_date_info","uc_additional_information","uc_credit_proof","uc_cancellation_user","uc_credit_logs","uc_document_info","uc_credit_basic_info_logs","uc_credit_v2_info_record","uc_ebank_logout_info","uc_first_withdraw","uc_first_income","uc_first_pay","uc_ebank_contract_record","uc_ebank_logout_record","uc_identity","uc_hl_input_record","uc_hl_loan_apply_record","uc_open_platform_auth_record","uc_open_platform_user_info","uc_hl_customer_identity","uc_hl_house_property","uc_hl_customer_profile","uc_login_record","uc_process_node","uc_ocr_identity","uc_ocr_bank_card","uc_password_signature_proof","uc_region","uc_sys_dict","uc_process_records","uc_send_sm_log","uc_signature_proof","uc_signature_record","uc_relation","uc_syscode","uc_basic_info","credit_basic_info","uc_hl_input_node")));

        syncTables.put("wallet", new HashSet<>(Arrays.asList("wlt_account","wlt_account_record","wlt_bill_record","wlt_cellphone_price_config","wlt_cellphone_relation","wlt_consumption_order","wlt_drawback_order","wlt_last_transaction_record","wlt_mch_bill_record_download","wlt_redpacket_order","wlt_sign_record","wlt_trans_order_record","wlt_open_tv_member_record","wlt_trans_order")));

        syncTables.put("payment", new HashSet<>(Arrays.asList("payment_file_info","maintain_check","payment_channel_code","payment_notice_config","payment_status_log","payment_exception_config","payment_callbank_info","payment_management_log","payment_task_info","recon_daily_detail","recon_task_record","recon_mistake_buffer_pool","recon_daily_mistake","recon_report_daily_summary","recon_daily_summary","payment_item")));

        syncTables.put("reward", new HashSet<>(Arrays.asList("red_packet_account","cms_red_packet","cms_red_packet_prize","cms_red_packet_scale","cms_vip_packet","red_packet_fail_record","red_packet_order_record","red_packet_prize_record","red_packet_user_relation_fail","red_packet_user_relation","vip_packet_user_relation","red_packet_statistics")));

        syncTables.put("loan",new HashSet<>(Arrays.asList("protocol_finger_print","repay_details_info","repay_details_relation","acct_info","acct_interest_detail","acct_prod_info","credit_black_list","credit_hit_black_log","credit_limit_info","credit_limit_info_expand","credit_white_list","fun_execute","gateway_merchant_config","limit_info","loan_contract_record","loan_details_info","loan_order_info","loan_order_info_expand","loan_order_info_relation_history","local_msg","repay_fee_change_record","repayment_plan","repayment_plan_mortgage_expand","report_credit_log","settle_trade_account","settle_trade_record","settle_trade_relative_record","stmt_bill","stmt_bill_detail","stmt_bill_detail_offset","stmt_bill_trans_record","stmt_cycle_info","trade_log","trade_order_detail","trade_order_info","trade_refund_record","trade_refund_record_log","user_black_white_info","user_third_data","user_third_relation","acct_fee_detail")));

        //忽略不存在的字段
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }


    private static boolean needSync(CanalData data){
        if(data.getDatabase()==null){
            return false;
        }
        HashSet tables  = syncTables.get(data.getDatabase());
        if(tables==null){
            return true;
        }
        if(tables.contains(data.getTable())){
            return true;
        }
        return false;
    }

    public static void save(ConsumerRecord<String, String> record){
        if(record==null)
            return;
        CanalData data = null ;
        try {
            data = mapper.readValue(record.value(),CanalData.class);
            if(needSync(data) ){
                DbUtil.save(data);
            }
        }catch (JsonProcessingException e){
            log.error("canalData parse error,{}",record.toString());
        }
        saveCheckPoint(record);
    }

    private static void saveCheckPoint(ConsumerRecord<String, String> record){

        offset.put(record.topic(),String.valueOf(record.offset()));
        //每30s保存checkpoint到redis
        if(System.currentTimeMillis() - checkPointTime > 30000){
            checkPointTime = System.currentTimeMillis();
            RedisUtil.hSet(Config.CHECKPOINT_KEY,offset);
            log.info(record.toString());
            log.info("offsets:{}",offset.toString());
        }
    }

    public static void main(String[] args) throws Exception {

        String s = "{\"data\":[{\"key\":\"a\",\"value\":\"test11\",\"test\":null},{\"key\":\"b\",\"value\":\"test22\"," +
                        "\"test\":null}],\"database\":\"public\",\"es\":1614236499000,\"id\":18413,\"isDdl\":false," +
                        "\"mysqlType\":{\"key\":\"varchar(254)\",\"value\":\"mediumtext\",\"test\":\"varchar(255)\"}," +
                        "\"old\":[{\"value\":\"1\"},{\"value\":\"2\"}],\"pkNames\":[\"key\"],\"sql\":\"\"," +
                        "\"sqlType\":{\"key\":12,\"value\":2005,\"test\":12},\"table\":\"setting1\"," +
                        "\"ts\":1614236499015,\"type\":\"UPDATE\"}\n" +
                        "{\"data\":[{\"SCHED_NAME\":\"MetabaseScheduler\"," +
                        "\"INSTANCE_NAME\":\"wk-datav-60001597035014971\",\"LAST_CHECKIN_TIME\":\"1614236501368\"," +
                        "\"CHECKIN_INTERVAL\":\"7500\"}],\"database\":\"metabase\",\"es\":1614236501000,\"id\":18414," +
                        "\"isDdl\":false,\"mysqlType\":{\"SCHED_NAME\":\"varchar(120)\",\"INSTANCE_NAME\":\"varchar" +
                        "(200)\",\"LAST_CHECKIN_TIME\":\"bigint(20)\",\"CHECKIN_INTERVAL\":\"bigint(20)\"}," +
                        "\"old\":[{\"LAST_CHECKIN_TIME\":\"1614236493867\"}],\"pkNames\":[\"SCHED_NAME\"," +
                        "\"INSTANCE_NAME\"],\"sql\":\"\",\"sqlType\":{\"SCHED_NAME\":12,\"INSTANCE_NAME\":12," +
                        "\"LAST_CHECKIN_TIME\":-5,\"CHECKIN_INTERVAL\":-5},\"table\":\"QRTZ_SCHEDULER_STATE\"," +
                        "\"ts\":1614236501518,\"type\":\"UPDATE\"}";

        String s1 = "{\"data\":null,\"database\":\"public\",\"es\":1614223867000,\"id\":16630,\"isDdl\":false," +
                "\"mysqlType\":null,\"old\":null,\"pkNames\":null,\"sql\":\"ALTER TABLE `setting1`\\r\\nADD COLUMN " +
                "`test`  varchar(255) NULL AFTER `value`\",\"sqlType\":null,\"table\":\"setting1\"," +
                "\"ts\":1614223867295,\"type\":\"ALTER\"}";
        String s2 = "{\"data\":[{\"key\":\"1\",\"value\":\"2\",\"test\":null}],\"database\":\"metabase\"," +
                "\"es\":1614234649000,\"id\":18127,\"isDdl\":false,\"mysqlType\":{\"key\":\"varchar(254)\"," +
                "\"value\":\"mediumtext\",\"test\":\"varchar(255)\"},\"old\":null,\"pkNames\":[\"key\"],\"sql\":\"\"," +
                "\"sqlType\":{\"key\":12,\"value\":2005,\"test\":12},\"table\":\"setting1\",\"ts\":1614234649735," +
                "\"type\":\"DELETE\"}";
        String s4 = "{\"data\":[{\"id\":\"1\",\"t\":null}],\"database\":\"bigdata\",\"es\":1614317982000," +
                "\"id\":30458,\"isDdl\":false,\"mysqlType\":{\"id\":\"int\",\"t\":\"datetime\"},\"old\":null," +
                "\"pkNames\":[\"id\"],\"sql\":\"\",\"sqlType\":{\"id\":4,\"t\":93},\"table\":\"test\"," +
                "\"ts\":1614317982393,\"type\":\"INSERT\"}";

        CanalData canalData = mapper.readValue(s4,CanalData.class);
        String sql = DbUtil.makeInsertSql(canalData);
        System.out.println(sql);
        //String sql1 = "delete from \"setting1\" where \"key\"='a' ;delete from \"setting1\" where " +
         //       "\"key\"='b' ;";
        //DbUtil.execute(sql);
        /*Map<String,String> map = new HashMap<>();
        map.put("A","1");
        map.put("b","2");
        map.put("c","3");
        RedisUtil.hSet("test",map);

        String a = " {\"data\":[{\"id\":\"37369462\",\"uuid\":\"e5772446c61f442ab0c0f215c17e89b3\"," +
                "\"external_no\":\"b876d1bc72f04e089f22c737e792d452\",\"mobile_phone\":null,\"source\":\"mgtv\"," +
                "\"valid_flag\":\"1\",\"insert_date\":\"2021-01-29 17:29:40\",\"modify_date\":\"2021-01-29 " +
                "17:29:40\"}],\"database\":\"uc\",\"es\":1611912580000,\"id\":20393568,\"isDdl\":false," +
                "\"mysqlType\":{\"id\":\"bigint(20)\",\"uuid\":\"varchar(32)\",\"external_no\":\"varchar(50)\"," +
                "\"mobile_phone\":\"varchar(20)\",\"source\":\"varchar(10)\",\"valid_flag\":\"tinyint(1)\"," +
                "\"insert_date\":\"datetime\",\"modify_date\":\"datetime\"},\"old\":null,\"pkNames\":[\"id\"]," +
                "\"sql\":\"\",\"sqlType\":{\"id\":-5,\"uuid\":12,\"external_no\":12,\"mobile_phone\":12," +
                "\"source\":12,\"valid_flag\":-6,\"insert_date\":93,\"modify_date\":93},\"table\":\"uc_relation\"," +
                "\"ts\":1611912580443,\"type\":\"INSERT\"}";

        String b = " {\"data\":[{\"id\":\"14365779\",\"process_id\":\"3114723\",\"node_code\":\"NODE_002\"," +
                "\"node_name\":\"face\",\"node_seq\":\"2\",\"jump_code\":\"https://wallet.mgtv.com/mgtv" +
                ".html#/facep\",\"start_time\":null,\"end_time\":null,\"risk_result\":\"0\",\"node_end\":\"2\"," +
                "\"device_id\":null,\"icon_image\":null,\"title\":\"人脸识别\",\"sub_title\":\"人脸识别\"," +
                "\"valid_flag\":\"1\",\"remark\":null,\"insert_date\":\"2021-01-29 17:26:35\"," +
                "\"modify_date\":\"2021-01-29 17:29:38\",\"operflag\":null}],\"database\":\"uc\"," +
                "\"es\":1611912578000,\"id\":20393563,\"isDdl\":false,\"mysqlType\":{\"id\":\"bigint(20)\"," +
                "\"process_id\":\"bigint(20)\",\"node_code\":\"varchar(20)\",\"node_name\":\"varchar(30)\"," +
                "\"node_seq\":\"tinyint(5)\",\"jump_code\":\"varchar(255)\",\"start_time\":\"datetime\"," +
                "\"end_time\":\"datetime\",\"risk_result\":\"tinyint(5)\",\"node_end\":\"tinyint(5)\"," +
                "\"device_id\":\"varchar(50)\",\"icon_image\":\"varchar(150)\",\"title\":\"varchar(50)\"," +
                "\"sub_title\":\"varchar(100)\",\"valid_flag\":\"tinyint(5)\",\"remark\":\"varchar(255)\"," +
                "\"insert_date\":\"datetime\",\"modify_date\":\"datetime\",\"operflag\":\"tinyint(5)\"}," +
                "\"old\":[{\"node_end\":\"0\",\"modify_date\":\"2021-01-29 17:26:35\"}],\"pkNames\":[\"id\"]," +
                "\"sql\":\"\",\"sqlType\":{\"id\":-5,\"process_id\":-5,\"node_code\":12,\"node_name\":12," +
                "\"node_seq\":-6,\"jump_code\":12,\"start_time\":93,\"end_time\":93,\"risk_result\":-6," +
                "\"node_end\":-6,\"device_id\":12,\"icon_image\":12,\"title\":12,\"sub_title\":12,\"valid_flag\":-6," +
                "\"remark\":12,\"insert_date\":93,\"modify_date\":93,\"operflag\":-6},\"table\":\"uc_process_node\"," +
                "\"ts\":1611912578536,\"type\":\"UPDATE\"}";



        ObjectMapper mapper = new ObjectMapper();
        //忽略不存在的字段
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        CanalData canalData = mapper.readValue(a,CanalData.class);

        CanalData canalData2 = mapper.readValue(b,CanalData.class);*/
/*

        String db = (String) value.get(DATABASE);
        String table  = (String) value.get(TABLE);
        String type  = (String) value.get(TYPE);

        List<String> pks = (List) value.get(PK);

        List<Map<String,String>> data =(List) value.get(DATA);

        Map<String,String> map = data.get(0);

        String sql = DbUtil.genInsertSql(db+"."+table,map);

        log.info(sql);*/

        /*log.info(value.toString());
        log.info(valueb.toString());*/
    }


}
