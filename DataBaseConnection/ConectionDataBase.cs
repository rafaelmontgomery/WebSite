using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Oracle.DataAccess.Client;

namespace DataBaseConnection
{
    public class ConectionDataBase
    {
        private readonly String ConnetionString = "Data Source=ORALOCAL;User Id=system;Password=montgomery;";
        private OracleCommand cmd;
        private OracleConnection conn;
        private OracleDataAdapter adapter;
        

        public ConectionDataBase()
        {
            conn = new OracleConnection(ConnetionString);
            cmd = new OracleCommand();            
            adapter = new OracleDataAdapter();
            
        }

        public int ExecuteCommand(String sql)
        {
            try
            {
                conn.Open();
                cmd.Connection = conn;
                cmd.CommandText = sql;
                return cmd.ExecuteNonQuery();                
            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message);
            }
            finally
            {
                conn.Close();
            }
        }

        public DataTable GetList(String sql)
        {
            try
            {
                var ListaData = new DataSet();
                conn.Open();
                cmd.Connection = conn;
                cmd.CommandText = sql;
                adapter = new OracleDataAdapter(cmd);
                adapter.Fill(ListaData);
                return ListaData.Tables[0];
            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message);
            }
            finally
            {
                conn.Close();
            }
        }

        public bool ExistTable(String TableName)
        {
            try
            {
                conn.Open();
                cmd.Connection = conn;
                cmd.CommandText = String.Format("Select 1 from {0} where 1 = 2", TableName);
                cmd.ExecuteScalar();
                return true;
            }
            catch (Exception)
            {
                return false;
            }
            finally
            {
                conn.Close();
            }
        }
       

        public Int64 GetSequence(String sequenceName)
        {
            try
            {
                conn.Open();
                cmd.Connection = conn;
                cmd.CommandText = String.Format("Select {0}.nextval from dual", sequenceName);
                OracleDataReader reader ;
                reader = cmd.ExecuteReader();
                if (reader.Read())
                    return Convert.ToInt64(reader.GetValue(0));
                else
                {
                    CreateSequence(sequenceName);
                    return 1;
                }
            }
            catch (Exception)
            {
                CreateSequence(sequenceName);
                return 1;
            }
            finally
            {
                conn.Close();
            }
            

        }

        private void CreateSequence(string sequenceName)
        {
            try
            {
                cmd.CommandText = String.Format("create sequence {0}\n" +
                                                "minvalue 1\n" + 
                                                "maxvalue 9999999999999999999999999999\n" + 
                                                "start with 2\n" + 
                                                "increment by 1\n" + 
                                                "nocache",sequenceName);

                cmd.ExecuteNonQuery();

            }
            catch (Exception)
            {
                
                throw;
            }
        }

        
    }
}
