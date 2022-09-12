package ru.omiplat.bgbilling;

import ru.bitel.bgbilling.common.BGException;
import ru.bitel.bgbilling.kernel.container.managed.ServerContext;
import ru.bitel.bgbilling.kernel.contract.api.common.bean.Contract;
import ru.bitel.bgbilling.kernel.contract.api.common.service.ContractService;
import ru.bitel.bgbilling.kernel.contract.balance.common.bean.Payment;
import ru.bitel.bgbilling.kernel.contract.balance.common.service.PaymentService;
import ru.bitel.bgbilling.kernel.script.server.dev.GlobalScriptBase;
import ru.bitel.bgbilling.server.util.Setup;
import ru.bitel.common.Utils;
import ru.bitel.common.model.Period;
import ru.bitel.common.sql.ConnectionSet;

import java.math.BigDecimal;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SyncBalanse extends GlobalScriptBase {
    public static void main(String[] args) {
        System.out.println(System.currentTimeMillis());
    }
    @Override
    public void execute(Setup setup, ConnectionSet set) throws Exception {
        Map<Integer, String> bgUsers = getBgUsers(set);
        System.out.println("bgUsers count "+ bgUsers.size());

        for (Map.Entry<Integer, String> bgUser : bgUsers.entrySet())
        {
            if(bgUser.getValue() == null)
                continue;
            String balance = getMikbillBalance(bgUser.getValue());
            System.out.println("bguser " +bgUser.getKey()+" mikbill_uid "+bgUser.getValue() + " balanse " + balance
            +" set_balance_status: " + (setBalance(bgUser.getKey(),balance,set)? "OK" : "FAIL")
            );
        }
    }


    public Map<Integer, String> getBgUsers(ConnectionSet set)
    {
        Connection con = set.getConnection();
        Map<Integer, String> bgUsers = new HashMap<>();
        try(Statement stmt = con.createStatement()){
            String req = "SELECT c.id, cpt.val as uid_mikbill FROM contract c\n" +
                    "left join contract_parameter_type_1 cpt \n" +
                    "on c.id = cpt.cid and cpt.pid = 8\n" +
                    "WHERE c.pgid = 2; \n";	//<<<<<<<<<<<<<<<<<<<<<<<<<<<<<< Ограничение на юриков
                    //+ " limit 2;"; // 								<<<<<<<<<<<<<<<<<<<<<<<<<<<<<< по 2 пользователя
            ResultSet rs = stmt.executeQuery(req);
            ResultSetMetaData metadata = rs.getMetaData();
            while (rs.next()) {
                Map<String,String> params = new HashMap<>();
                bgUsers.put(rs.getInt(1),rs.getString(2));
            }
        }catch(Exception e){
            e.printStackTrace();

        }
        return bgUsers;
    }
    public static String getMikbillBalance(String id)
    {
        String res = "";

        try
        {
            Connection connection = DriverManager.getConnection("jdbc:mysql://x.x.x.x:3306/mikbill","xxxx","xxxx");
            Statement stmt = connection.createStatement();
            String req = "SELECT u.deposit FROM users_all u\n" +
                    "WHERE u.uid  = " + id +";";
            ResultSet rs = stmt.executeQuery(req);

            while (rs.next()) {
                return rs.getString(1);
            }
        }catch(Exception e){
            e.printStackTrace();
        }
        return res;
    }
    public static boolean setBalance(int id, String balanceIn, ConnectionSet set) throws BGException {
        ServerContext serverContext = ServerContext.get();
        Connection con = set.getConnection();
        ContractService contractService = serverContext.getService(ContractService.class, 0);
        PaymentService paymentService = serverContext.getService(PaymentService.class, 0);
        //Получаем существующие платежи
        List<Payment> pays = paymentService.paymentList(id,new Period((java.util.Date) null,null),1,null);
        //Удаляем не нужные типы платежей
        if(pays != null)
        {
            pays.removeIf(pp -> pp.getTypeId() != 10);
            //Перед записью удаляем все другие платежи этого типа

            for (Payment pp : pays) {
                paymentService.paymentDelete(pp.getContractId(), pp.getId());
            }
        }

        BigDecimal balance = Utils.parseBigDecimal(balanceIn, BigDecimal.ZERO);
        if (balance.compareTo(BigDecimal.ZERO) != 0) {
            Contract contract = contractService.contractGet(id);
            if (contract == null) {
                System.out.println("Contract " + id + " not found");
                return false;
            }
            Payment payment = new Payment();
            payment.setContractId(contract.getId());
            payment.setDate(new Date(Long.parseLong("1663014614832")));
            payment.setTypeId(10);
            payment.setSum(balance);
            payment.setComment("Перенос вх. остатков");
            paymentService.paymentUpdate(payment, "");
        }
        return true;
    }
}
