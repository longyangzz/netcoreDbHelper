using CodeM.Common.DbHelper;
using Npgsql;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Numerics;
using System.Threading.Tasks;

namespace UnitTest
{
    /// <summary>
    /// PostgreSQL数据库测试
    /// </summary>
    public class PostgresTest
    {
        public PostgresTest()
        {
            DbUtils.RegisterDbProvider("postgres", "Npgsql.NpgsqlFactory, Npgsql");
            DbUtils.AddDataSource("postgres_test", "postgres", "Host=111.198.29.34:5444;Database=mxpt_business_databases;User Id=postgres;Password=Mxpt@2024#1;");
        }

        [Test]
        public void Order1_CreateTable()
        {
            string sql = "drop table test";
            int result = DbUtils.ExecuteNonQuery("postgres_test", sql);
            Assert.IsTrue(result == 0);

            sql = "Create Table test(id int not null primary key, name varchar(64), age int, address varchar(255))";
            result = DbUtils.ExecuteNonQuery("postgres_test", sql);
            Assert.IsTrue(result == 0);
            string comment = "COMMENT ON COLUMN test.name IS '名称'";
            result = DbUtils.ExecuteNonQuery("postgres_test", comment);
            Assert.IsTrue(result == 0);
        }

        [Test]
        public void Order2_InsertRecord()
        {
            

            //多线程并发调用sql查询
            int threadCount = 50;
            var tasks = new Task[threadCount];
            for (int index = 0; index < threadCount; index++)
            {
                int i = index;
                tasks[index] = Task.Run(() =>
                {

                    string sql = String.Format("Insert Into test (id, name, age, address) Values({0}, 'wangxm', 18, '河北保定')", i + 1);
                    int result = DbUtils.ExecuteNonQuery("postgres_test", sql);
                    Assert.IsTrue(result == 1);

                });

            }
            // 等待所有任务完成
            Task.WhenAll(tasks).Wait();
        }

        [Test]
        public void Order3_InsertRecord2()
        {
            try
            {
                string sql = "Insert Into test (id, name, age, address) Values(1, 'wangxm', 18, '河北保定')";
                int result = DbUtils.ExecuteNonQuery("postgres_test", sql);
                Assert.Fail("Insert Error.");
            }
            catch (Exception e)
            {
                Assert.IsTrue(e.Message.Contains("重复"));
            }
        }

        [Test]
        public void Order4_QueryRecord()
        {
            //多线程并发调用sql查询
            int threadCount = 5;
            var tasks = new Task[threadCount];
            for (int index = 0; index < threadCount; index++)
            {
                int i = index;
                tasks[index] = Task.Run(() =>
                {

                    string sql = "Select * From test Where id=1";
                    DbDataReader dr = DbUtils.ExecuteDataReader("postgres_test", sql);
                    Assert.IsTrue(dr.HasRows);
                    Assert.IsTrue(dr.Read());
                    string name = dr.GetString(1);
                    Assert.AreEqual(name, "wangxm");
                    dr.Close();

                });

            }
            // 等待所有任务完成
            Task.WhenAll(tasks).Wait();

            
        }

        [Test]
        public void Order5_DropTable()
        {
            string sql = "delete from test";
            int result = DbUtils.ExecuteNonQuery("postgres_test", sql);
            //Assert.IsTrue(result == 0);
        }

        //test批量写出
        [Test]
        public void Order6_BatchInsert()
        {
            //id, name, age, address 
            //1, 'wangxm', 18, '河北保定'
            //模拟生成DataTable数据
            DataTable dataTable = new DataTable();
            dataTable.Columns.Add("id", typeof(int));
            dataTable.Columns.Add("name", typeof(string));
            dataTable.Columns.Add("age", typeof(int));
            dataTable.Columns.Add("address", typeof(string));
            //模拟生成1000条数据
            for (int i = 0; i < 1000; i++)
            {
                DataRow row = dataTable.NewRow();
                row["id"] = i + 1;
                row["name"] = "wangxm";
                row["age"] = 18;
                row["address"] = "河北保定";
                dataTable.Rows.Add(row);
            }
            BulkToDB("postgres_test", dataTable, "test");
        }

        public void BulkToDB(String connection, DataTable dataTable, string tbname)
        {

            try
            {
                //** 测试示例
                //dataTable.Clear();
                //DataRow dr_rivl = dataTable.NewRow();
                //dr_rivl[1] = "1";
                //dr_rivl[2] = "555";
                //dr_rivl[3] = DateTime.Now;
                //dr_rivl[4] = "5555";
                //dr_rivl[5] = 0.2;
                //dr_rivl[6] = 0;
                //dr_rivl[7] = DateTime.Now;

                //dr_rivl[8] = DateTime.Now;    //! 峰值流量时间
                //dr_rivl[9] = 5.6;
                //dr_rivl[10] = "2021-08-08 00:00";
                //dr_rivl[12] = 33;
                //dataTable.Rows.Add(dr_rivl);
                //** 测试示例
                if (dataTable != null && dataTable.Rows.Count != 0)
                {
                    //目标表中所有列名
                    String sqlTable = String.Format("Select column_name, data_type from information_schema.COLUMNS Where table_name ='{0}'", tbname.ToLower());
                    DbDataReader dset = DbUtils.ExecuteDataReader(connection, sqlTable);

                    //获取所有列名 - 第0列。存储到list中
                    List<string> lsColNames = new List<string>();
                    List<string> lgnoreColNames = new List<string>();

                    //对应列名的类型
                    List<string> lsColDataType = new List<string>();

                    //获取列名-数据库中表的列名
                    while (dset.Read())
                    {
                        for (int i = 0; i < 1; i++)
                        {
                            if (dset.GetValue(i).ToString().ToLower().Equals("id44") || dset.GetValue(i).ToString().ToLower().Equals("intime"))
                            {
                                lgnoreColNames.Add(dset.GetValue(i).ToString().ToLower());
                            }
                            else
                            {
                                //if (tbname.ToLower().Equals("hsfx_rivl_flow_plus_forecast"))
                                //{
                                //    if (lsColNames.Count == 2)
                                //    {
                                //        break;
                                //    }
                                //}
                                //! 判断名字 在 datatable中存在，则添加
                                if (dataTable.Columns.Contains(dset.GetValue(i).ToString().ToLower()))
                                {
                                    lsColNames.Add(dset.GetValue(i).ToString().ToLower());
                                    lsColDataType.Add(dset.GetValue(i+1).ToString().ToLower());
                                }
                            }
                        }
                    }
                    

                    var commandFormat = string.Format("copy {0}({1}) FROM STDIN BINARY", tbname.ToLower(), string.Join(",", lsColNames));
                    NpgsqlConnection npgsqlConnection = DbUtils.GetConnection(connection) as NpgsqlConnection;
                    npgsqlConnection.Open();
                    using (var writer = npgsqlConnection.BeginBinaryImport(commandFormat))
                    {
                        foreach (DataRow row in dataTable.Rows)
                        {
                            writer.StartRow();
                            foreach (String collName in lsColNames)
                            {
                                //根据列名获取列的数据类型
                                DataColumn coll = dataTable.Columns[collName];
                                var colldbtype = coll.DataType.Name.ToString();

                                //某列在数据库中的数据类型
                                //判断当前值与 数据表中的数据类型是否一致
                                int indexColName = lsColNames.IndexOf(coll.ColumnName.ToLower());
                                String colDataType = lsColDataType[indexColName];

                                NpgsqlTypes.NpgsqlDbType pgtype = NpgsqlTypes.NpgsqlDbType.Bigint;

                                // 判断integer、bigint、numeric、double precision、character varying、timestamp
                                if (colDataType.ToLower().Contains("integer"))
                                {
                                    pgtype = NpgsqlTypes.NpgsqlDbType.Integer;
                                    int value = 0;
                                    if (int.TryParse(row[coll.ColumnName].ToString(), out value))
                                    {
                                        writer.Write(value, pgtype);
                                    }
                                    else
                                    {
                                        writer.Write(DBNull.Value, pgtype);
                                    }

                                }
                                else if (colDataType.ToLower().Contains("bigint"))
                                {
                                    pgtype = NpgsqlTypes.NpgsqlDbType.Bigint;
                                    BigInteger value = 0;
                                    if (BigInteger.TryParse(row[coll.ColumnName].ToString(), out value))
                                    {
                                        writer.Write(value, pgtype);
                                    }
                                    else
                                    {
                                        writer.Write(DBNull.Value, pgtype);
                                    }
                                }
                                else if (colDataType.ToLower().Contains("numeric"))
                                {
                                    pgtype = NpgsqlTypes.NpgsqlDbType.Numeric;
                                    Decimal value = 0;
                                    if (Decimal.TryParse(row[coll.ColumnName].ToString(), out value))
                                    {
                                        writer.Write(value, pgtype);
                                    }
                                    else
                                    {
                                        writer.Write(DBNull.Value, pgtype);
                                    }
                                }
                                else if (colDataType.ToLower().Contains("double precision"))
                                {
                                    pgtype = NpgsqlTypes.NpgsqlDbType.Double;

                                    Double value = 0;
                                    if (Double.TryParse(row[coll.ColumnName].ToString(), out value))
                                    {
                                        writer.Write(value, pgtype);
                                    }
                                    else
                                    {
                                        writer.Write(DBNull.Value, pgtype);
                                    }
                                }
                                else if (colDataType.ToLower().Contains("character varying"))
                                {
                                    pgtype = NpgsqlTypes.NpgsqlDbType.Varchar;
                                    if (row[coll.ColumnName] == DBNull.Value)
                                    {
                                        writer.Write(DBNull.Value, pgtype);
                                    }
                                    else
                                    {

                                        writer.Write(row[coll.ColumnName].ToString(), pgtype);
                                    }

                                }
                                else if (colDataType.ToLower().Contains("timestamp"))
                                {
                                    pgtype = NpgsqlTypes.NpgsqlDbType.Timestamp;
                                    DateTime value = new DateTime();
                                    if (DateTime.TryParse(row[coll.ColumnName].ToString(), out value))
                                    {
                                        writer.Write(value, pgtype);
                                    }
                                    else
                                    {
                                        writer.Write(DBNull.Value, pgtype);
                                    }
                                }
                                else if (colDataType.ToLower().Contains("text"))
                                {

                                    pgtype = NpgsqlTypes.NpgsqlDbType.Text;
                                    if (row[coll.ColumnName] == DBNull.Value)
                                    {
                                        writer.Write(DBNull.Value, pgtype);
                                    }
                                    else
                                    {
                                        writer.Write(row[coll.ColumnName].ToString(), pgtype);
                                    }

                                }
                                else
                                {
                                    pgtype = NpgsqlTypes.NpgsqlDbType.Varchar;
                                    if (row[coll.ColumnName] == DBNull.Value)
                                    {
                                        writer.Write(DBNull.Value, pgtype);
                                    }
                                    else
                                    {
                                        writer.Write(row[coll.ColumnName], pgtype);
                                    }

                                }
                            }
                        }
                        writer.Complete();
                    }

                }


            }
            catch (Exception)
            {
                throw;
            }
            finally
            {

            }

        }
    }
}
