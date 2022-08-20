package com.bode.city700.integrition;

import java.beans.Beans;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.Vector;

import javax.naming.InitialContext;
import javax.sql.DataSource;

// Referenced classes of package com.bode.city700.integrition:
// LogData, Bytes, ReadyStatement

public class DlxxDBAccess extends Beans
{

	static DataSource m_ds = null;
	static
	{
		try
		{
			Class.forName("COM.ibm.db2.jdbc.app.DB2Driver").newInstance();
		}
		catch (Exception e)
		{
			System.out.println("\n  Error loading DB2 Driver...\n");
			System.out.println(e);
		}
	}

	public static String getSingleValue(String strSql) throws Exception
	{
		DlxxDBAccess dba = null;
		DlxxDBAccess dbaMaster = null;
		String strValue = null;
		try
		{
			dbaMaster = new DlxxDBAccess();
			dba = dbaMaster.prepareSql(strSql, false);
			dba.execute();
			if (dba.next())
			{
				strValue = dba.getString(1);
			}
		}
		finally
		{
			if (dba != null)
			{
				dba.close();
			}
			if (dbaMaster != null)
			{
				dbaMaster.close();
			}
		}
		return strValue;
	}

	Vector<DlxxDBAccess> m_arChildren;

	Vector<LogData> m_arLogData;

	Vector<ReadyStatement> m_arReadyStmt;

	boolean m_bIsChild;

	boolean m_bUpdate;

	Connection m_conn;

	DlxxDBAccess m_dbaParent;

	int m_nParams;

	ReadyStatement m_readyStmt;

	ResultSet m_rst;

	PreparedStatement m_stmtPrep;

	String m_strSql;

	public DlxxDBAccess() throws Exception
	{
		m_conn = null;
		m_arChildren = null;
		m_bIsChild = false;
		m_bUpdate = false;
		m_dbaParent = null;
		m_stmtPrep = null;
		m_arReadyStmt = null;
		m_readyStmt = null;
		m_rst = null;
		m_strSql = null;
		m_arLogData = null;
		m_nParams = 0;
		m_arChildren = new Vector<DlxxDBAccess>();
		m_conn = getConnection();
		m_arReadyStmt = new Vector<ReadyStatement>();
	}

	protected DlxxDBAccess(Connection conn, String strSql, boolean bUpdate,
			DlxxDBAccess dbaParent) throws Exception
	{
		m_conn = null;
		m_arChildren = null;
		m_bIsChild = false;
		m_bUpdate = false;
		m_dbaParent = null;
		m_stmtPrep = null;
		m_arReadyStmt = null;
		m_readyStmt = null;
		m_rst = null;
		m_strSql = null;
		m_arLogData = null;
		m_nParams = 0;
		m_bUpdate = bUpdate;
		m_bIsChild = true;
		m_dbaParent = dbaParent;
		m_strSql = strSql;
		if (bUpdate)
		{
			m_nParams = 0;
			strSql.length();
			for (int nStart = strSql.indexOf('?'); nStart != -1; nStart = strSql
					.indexOf('?', nStart + 1))
			{
				m_nParams++;
			}

			m_arLogData = new Vector<LogData>();
			LogData log = new LogData();
			log.setParamCount(m_nParams);
			m_arLogData.add(log);
		}
		if ((strSql != null) && (strSql.indexOf('?') != -1))
		{
			m_stmtPrep = conn.prepareStatement(strSql);
		}
	}

	boolean canCreateNewLog()
	{
		LogData log = getLastLog();
		return m_bUpdate && ((log == null) || log.m_bExecuted);
	}

	public void close()
	{
		for (int i = 0; (m_arChildren != null) && (i < m_arChildren.size()); i++)
		{
			(m_arChildren.elementAt(i)).close();
		}

		try
		{
			if (m_conn != null)
			{
				m_conn.rollback();
			}
		}
		catch (Throwable throwable)
		{
		}
		try
		{
			if (m_rst != null)
			{
				m_rst.close();
			}
		}
		catch (Throwable throwable1)
		{
		}
		try
		{
			if (m_stmtPrep != null)
			{
				m_stmtPrep.close();
			}
		}
		catch (Throwable throwable2)
		{
		}
		try
		{
			if (m_readyStmt != null)
			{
				m_readyStmt.retire();
			}
		}
		catch (Throwable throwable3)
		{
		}
		for (int i = 0; (m_arReadyStmt != null) && (i < m_arReadyStmt.size()); i++)
		{
			ReadyStatement readyStmt = m_arReadyStmt.elementAt(i);
			readyStmt.close();
		}

		try
		{
			if (m_conn != null)
			{
				m_conn.close();
			}
		}
		catch (Throwable throwable4)
		{
		}
		m_rst = null;
		m_stmtPrep = null;
		m_readyStmt = null;
		m_arReadyStmt = null;
		m_arChildren = null;
		m_conn = null;
		m_dbaParent = null;
	}

	public void commit() throws Throwable
	{
		if (("" == null) && !m_bIsChild)
		{
			for (int i = 0; (m_arChildren != null) && (i < m_arChildren.size()); i++)
			{
				DlxxDBAccess child = m_arChildren.elementAt(i);
				if (child.m_bUpdate)
				{
					if (m_stmtPrep == null)
					{
						m_stmtPrep = m_conn
								.prepareStatement("insert into sqllog(sequenceid, sqlstring, paramtype, paramdata, paramcount, sett"
										+ "ime, outputed) values(GENERATE_UNIQUE(), ?, ?, ?, ?, current timestamp, 0)");
					}
					m_stmtPrep.setString(1, child.m_strSql);
					m_stmtPrep.setInt(4, child.m_nParams);
					if (child.m_nParams == 0)
					{
						m_stmtPrep.setString(2, null);
						m_stmtPrep.setString(3, null);
						m_stmtPrep.executeUpdate();
					}
					else
					{
						for (int j = 0; (child.m_arLogData != null)
								&& (j < child.m_arLogData.size()); j++)
						{
							LogData log = child.m_arLogData.elementAt(j);
							if (log.m_bExecuted)
							{
								m_stmtPrep.setString(2, log
										.getParamTypeString());
								ByteArrayOutputStream strmOut = null;
								ObjectOutputStream op = null;
								try
								{
									strmOut = new ByteArrayOutputStream();
									op = new ObjectOutputStream(strmOut);
									for (int k = 0; k < child.m_nParams; k++)
									{
										Object objData = log.m_arData[k];
										op.writeObject(new Integer(
												objData != null ? 1 : 0));
										if (objData != null)
										{
											op.writeObject(objData);
										}
									}

									op.flush();
									strmOut.close();
									m_stmtPrep.setBytes(3, strmOut
											.toByteArray());
									m_stmtPrep.executeUpdate();
								}
								catch (Throwable e)
								{
									e.printStackTrace();
									throw e;
								}
								finally
								{
									if (strmOut != null)
									{
										strmOut.close();
									}
								}
							}
						}

					}
				}
			}

		}
		m_conn.commit();
	}

	public void debugPrint()
	{
		System.out.println("conn = " + m_conn);
		if (m_readyStmt != null)
		{
			System.out.println("readystmt = " + m_readyStmt.m_stmt + " ready: "
					+ m_readyStmt.m_bReady);
		}
	}

	public int execute() throws Exception
	{
		int nRet = -1;
		if (m_stmtPrep != null)
		{
			if (m_stmtPrep.execute())
			{
				m_rst = m_stmtPrep.getResultSet();
			}
			else
			{
				nRet = m_stmtPrep.getUpdateCount();
			}
		}
		else if (m_bIsChild)
		{
			m_readyStmt = m_dbaParent.getStatement();
			if (!m_bUpdate)
			{
				m_rst = m_readyStmt.m_stmt.executeQuery(m_strSql);
			}
			else
			{
				nRet = m_readyStmt.m_stmt.executeUpdate(m_strSql);
			}
		}
		LogData log = getLastLog();
		if (canCreateNewLog())
		{
			m_arLogData.add(getLastLog());
		}
		if ((log != null) && m_bUpdate)
		{
			log.m_bExecuted = true;
		}
		return nRet;
	}

	public int executeUpdate(String strSql) throws Exception
	{
		DlxxDBAccess dbaChild = prepareSql(strSql, true);
		int nRet = dbaChild.execute();
		dbaChild.close();
		return nRet;
	}

	@Override
	protected void finalize() throws Throwable
	{
		close();
	}

	public byte[] getBytes(int nIndex) throws Exception
	{
		return m_rst.getBytes(nIndex);
	}

	public byte[] getBytes(String strIndex) throws Exception
	{
		return m_rst.getBytes(strIndex);
	}

	public Connection getConnection() throws Exception
	{
		InitialContext ctx = null;
		Connection conn = null;
		try
		{
			if (m_ds == null)
			{
				ctx = new InitialContext();
				m_ds = (DataSource) ctx.lookup("java:/comp/env/jdbc/city800");
			}
			conn = m_ds.getConnection();
			conn.setAutoCommit(false);
		}
		finally
		{
			if (ctx != null)
			{
				ctx.close();
			}
		}
		return conn;
	}

	public Date getDate(int nIndex) throws Exception
	{
		return m_rst.getDate(nIndex);
	}

	public Date getDate(String strIndex) throws Exception
	{
		return m_rst.getDate(strIndex);
	}

	public double getDouble(int nIndex) throws Exception
	{
		return m_rst.getDouble(nIndex);
	}

	public double getDouble(String strIndex) throws Exception
	{
		return m_rst.getDouble(strIndex);
	}

	public float getFloat(int nIndex) throws Exception
	{
		return m_rst.getFloat(nIndex);
	}

	public float getFloat(String strIndex) throws Exception
	{
		return m_rst.getFloat(strIndex);
	}

	public int getInt(int nIndex) throws Exception
	{
		return m_rst.getInt(nIndex);
	}

	public int getInt(String strIndex) throws Exception
	{
		return m_rst.getInt(strIndex);
	}

	LogData getLastLog()
	{
		if ((m_arLogData == null) || (m_arLogData.size() == 0))
		{
			return null;
		}
		else
		{
			return m_arLogData.elementAt(m_arLogData.size() - 1);
		}
	}

	public long getLong(int nIndex) throws Exception
	{
		return m_rst.getLong(nIndex);
	}

	public long getLong(String strIndex) throws Exception
	{
		return m_rst.getLong(strIndex);
	}

	public String getSingleValue2(String strSql) throws Exception
	{
		DlxxDBAccess dba = null;
		String strValue = null;
		try
		{
			dba = prepareSql(strSql, false);
			dba.execute();
			if (dba.next())
			{
				strValue = dba.getString(1);
			}
		}
		finally
		{
			if (dba != null)
			{
				dba.close();
			}
		}
		return strValue;
	}

	synchronized ReadyStatement getStatement() throws Exception
	{
		ReadyStatement readyStmt = null;
		for (int i = 0; (m_arReadyStmt != null) && (i < m_arReadyStmt.size()); i++)
		{
			readyStmt = m_arReadyStmt.elementAt(i);
			if (readyStmt.m_bReady)
			{
				readyStmt.m_bReady = false;
				return readyStmt;
			}
		}

		readyStmt = new ReadyStatement();
		readyStmt.create(m_conn);
		m_arReadyStmt.add(readyStmt);
		return readyStmt;
	}

	public String getString(int nIndex) throws Exception
	{
		String strValue = m_rst.getString(nIndex);
		if (strValue != null)
		{
			return strValue;
		}
		else
		{
			return "";
		}
	}

	public String getString(String strIndex) throws Exception
	{
		String strValue = m_rst.getString(strIndex);
		if (strValue != null)
		{
			return strValue;
		}
		else
		{
			return "";
		}
	}

	public boolean next() throws Exception
	{
		return m_rst.next();
	}

	public DlxxDBAccess prepareSql(String strSql) throws Exception
	{
		return prepareSql(strSql, false);
	}

	public DlxxDBAccess prepareSql(String strSql, boolean bUpdate)
			throws Exception
	{
		DlxxDBAccess dbaChild = new DlxxDBAccess(m_conn, strSql, bUpdate, this);
		m_arChildren.add(dbaChild);
		return dbaChild;
	}

	public void rollback() throws Exception
	{
		m_conn.rollback();
	}

	public void setBytes(int nIndex, byte arData[]) throws Exception
	{
		m_stmtPrep.setBytes(nIndex, arData);
		if (m_bUpdate)
		{
			LogData log = null;
			if (canCreateNewLog())
			{
				log = new LogData();
				log.setParamCount(m_nParams);
				m_arLogData.add(log);
			}
			else
			{
				log = getLastLog();
			}
			log.m_arParamType[nIndex - 1] = 4;
			if (arData != null)
			{
				log.m_arData[nIndex - 1] = new Bytes(arData);
			}
			else
			{
				log.m_arData[nIndex - 1] = null;
			}
		}
	}

	public void setDate(int nIndex, Date dtValue) throws Exception
	{
		m_stmtPrep.setDate(nIndex, dtValue);
		if (m_bUpdate)
		{
			LogData log = null;
			if (canCreateNewLog())
			{
				log = new LogData();
				log.setParamCount(m_nParams);
				m_arLogData.add(log);
			}
			else
			{
				log = getLastLog();
			}
			log.m_arParamType[nIndex - 1] = 2;
			log.m_arData[nIndex - 1] = dtValue;
		}
	}

	public void setInt(int nIndex, int nValue) throws Exception
	{
		m_stmtPrep.setInt(nIndex, nValue);
		if (m_bUpdate)
		{
			LogData log = null;
			if (canCreateNewLog())
			{
				log = new LogData();
				log.setParamCount(m_nParams);
				m_arLogData.add(log);
			}
			else
			{
				log = getLastLog();
			}
			log.m_arParamType[nIndex - 1] = 0;
			log.m_arData[nIndex - 1] = new Integer(nValue);
		}
	}

	public void setString(int nIndex, String strValue) throws Exception
	{
		m_stmtPrep.setString(nIndex, strValue);
		if (m_bUpdate)
		{
			LogData log = null;
			if (canCreateNewLog())
			{
				log = new LogData();
				log.setParamCount(m_nParams);
				m_arLogData.add(log);
			}
			else
			{
				log = getLastLog();
			}
			log.m_arParamType[nIndex - 1] = 1;
			log.m_arData[nIndex - 1] = strValue;
		}
	}

	public void setTimestamp(int nIndex, Timestamp tmValue) throws Exception
	{
		m_stmtPrep.setTimestamp(nIndex, tmValue);
		if (m_bUpdate)
		{
			LogData log = null;
			if (canCreateNewLog())
			{
				log = new LogData();
				log.setParamCount(m_nParams);
				m_arLogData.add(log);
			}
			else
			{
				log = getLastLog();
			}
			log.m_arParamType[nIndex - 1] = 3;
			log.m_arData[nIndex - 1] = tmValue;
		}
	}

	public synchronized void synchFromFile(String strFile) throws Exception
	{
		FileInputStream istream = null;
		ObjectInputStream ip = null;
		ReadyStatement readyStmt = null;
		try
		{
			istream = new FileInputStream(strFile);
			ip = new ObjectInputStream(istream);
			for (int nCount = ip.readInt(); nCount > 0; nCount--)
			{
				String strSqlString = (String) ip.readObject();
				if (strSqlString.indexOf('?') != -1)
				{
					m_stmtPrep = m_conn.prepareStatement(strSqlString);
				}
				else if (readyStmt == null)
				{
					readyStmt = getStatement();
				}
				String strParamType = (String) ip.readObject();
				if (strParamType != null)
				{
					int nEnd = strParamType.indexOf('^');
					int nIndex = 1;
					for (; nEnd != -1; nEnd = strParamType.indexOf('^'))
					{
						String strType = strParamType.substring(0, nEnd);
						int nType = (new Integer(strType)).intValue();
						Integer intFlag = (Integer) ip.readObject();
						if (intFlag.intValue() != 0)
						{
							switch (nType)
							{
							case 1: // '\001'
							case 2: // '\002'
							case 3: // '\003'
								m_stmtPrep.setObject(nIndex++, ip.readObject());
								break;

							case 0: // '\0'
								Integer nData = (Integer) ip.readObject();
								m_stmtPrep.setInt(nIndex++, nData.intValue());
								break;

							case 4: // '\004'
								Bytes bsData = (Bytes) ip.readObject();
								m_stmtPrep
										.setBytes(nIndex++, bsData.getBytes());
								break;

							default:
								nIndex++;
								break;
							}
						}
						else
						{
							nIndex++;
						}
						if (++nEnd >= strParamType.length())
						{
							break;
						}
						strParamType = strParamType.substring(nEnd);
					}

				}
				if (m_stmtPrep != null)
				{
					m_stmtPrep.executeUpdate();
					m_stmtPrep.close();
				}
				else
				{
					readyStmt.m_stmt.executeUpdate(strSqlString);
				}
			}

		}
		finally
		{
			if (istream != null)
			{
				istream.close();
			}
			if (readyStmt != null)
			{
				readyStmt.retire();
			}
		}
	}

	public synchronized void synchToFile(String strFile) throws Exception
	{
		FileOutputStream istream = null;
		ObjectOutputStream op = null;
		int nCount = (new Integer(
				getSingleValue("select count(*) from sqllog where not outputed = 1")))
				.intValue();
		if (nCount == 0)
		{
			return;
		}
		ReadyStatement readyStmt = null;
		try
		{
			istream = new FileOutputStream(strFile, false);
			op = new ObjectOutputStream(istream);
			op.writeInt(nCount);
			readyStmt = getStatement();
			ResultSet rst;
			for (rst = readyStmt.m_stmt
					.executeQuery("select sqlstring, paramtype, paramdata, paramcount from sqllog where not outpute"
							+ "d = 1 order by settime, sequenceid"); rst.next();)
			{
				op.writeObject(rst.getString(1));
				op.writeObject(rst.getString(2));
				byte arData[] = rst.getBytes(3);
				int nParams = rst.getInt(4);
				if (arData != null)
				{
					ByteArrayInputStream ips = new ByteArrayInputStream(arData);
					ObjectInputStream oip = new ObjectInputStream(ips);
					for (int i = 0; i < nParams; i++)
					{
						Integer intFlag = (Integer) oip.readObject();
						op.writeObject(intFlag);
						if (intFlag.intValue() != 0)
						{
							op.writeObject(oip.readObject());
						}
					}

					ips.close();
				}
			}

			op.flush();
			rst.close();
			readyStmt.m_stmt
					.executeUpdate("update sqllog set outputed = 1 where not outputed = 1");
		}
		finally
		{
			if (istream != null)
			{
				istream.close();
			}
			if (readyStmt != null)
			{
				readyStmt.retire();
			}
		}
	}
}
