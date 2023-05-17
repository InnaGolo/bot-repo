/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.poe.kmltodb;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author dit82
 */
public class DBUpdater {
    
    public static void PushKLToDB(Connection connection, List<KL> list, String filial) throws SQLException{
            
           PreparedStatement preparedStatement;
           preparedStatement = connection.prepareStatement("DELETE FROM gis_kl_coord WHERE tplnr like '%F-"+filial+"%'");
           for (int i = 0; i < list.size(); i++) {
                
                        preparedStatement = connection.prepareStatement("INSERT INTO public.gis_kl_coord (tplnr, coord) SELECT tplnr, ?::jsonb FROM gis_kl WHERE invnr = ? and invnr <> '' and (tplnr LIKE '%F-"+filial+"%' or tplnr LIKE 'PO-F-31%');");//, ?::jsonb;");
                        preparedStatement.setString(1, list.get(i).getCoord());
                        preparedStatement.setString(2, list.get(i).getNumber());

                        preparedStatement.executeUpdate();
                        preparedStatement.close();
                    }
                
            }
    
    
    public static void PushPLToDB(Connection connection, List<PL> list) throws SQLException{
        PreparedStatement preparedStatement;
        for (int i = 0; i< list.size(); i++) {
            preparedStatement = connection.prepareStatement("UPDATE public.lep_link SET mast = ?::jsonb WHERE name = ?");
            
            preparedStatement.setString(1, list.get(i).getCoord());
            preparedStatement.setString(2, list.get(i).getName());
            preparedStatement.executeUpdate();
            preparedStatement.close();
        }
    }
    
    public static void PushTPToDB(Connection connection, List<TP> list) throws SQLException{
        PreparedStatement preparedStatement;
        for (int i = 0; i< list.size(); i++) {
            preparedStatement = connection.prepareStatement("UPDATE public.zpm_gis_tp_v2 SET lat = ? , lng = ? WHERE tplnr = ?");
            preparedStatement.setDouble(1, list.get(i).getLat());
            preparedStatement.setDouble(2, list.get(i).getLng());
            preparedStatement.setString(3, list.get(i).getTplnr());
            preparedStatement.executeUpdate();
            preparedStatement.close();
        }
    }
    
    public static void PushMbpToDB(Connection connection, List<MBP> list) throws SQLException{
        PreparedStatement preparedStatement;
        for (int i = 0; i< list.size(); i++) {
            preparedStatement = connection.prepareStatement("UPDATE public.reg_point_lep SET lat = ?, lng =? WHERE name like ?");
            preparedStatement.setDouble(1, list.get(i).getLat());
            preparedStatement.setDouble(2, list.get(i).getLng());
            preparedStatement.setString(3, "%"+list.get(i).getName());
            preparedStatement.executeUpdate();
            preparedStatement.close();
        }
    }
    
    public static void PushKvToDB(Connection connection, List<PsKvadrat> list) throws SQLException{
        PreparedStatement preparedStatement;
        for (int i = 0; i< list.size(); i++) {
            preparedStatement = connection.prepareStatement("INSERT INTO public.ps_kontur (tplnr, coord) VALUES (?, ?::jsonb)");
            preparedStatement.setString(1, list.get(i).getTplnr());
            preparedStatement.setString(2, list.get(i).getCoord());
            preparedStatement.executeUpdate();
            preparedStatement.close();
        }
    }
    
}
