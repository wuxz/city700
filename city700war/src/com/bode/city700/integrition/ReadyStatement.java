package com.bode.city700.integrition;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.Statement;

class ReadyStatement implements Serializable
{

	/**
	 * 
	 */
	private static final long serialVersionUID = -2408065296210845276L;

	boolean m_bReady;

	Statement m_stmt;

	ReadyStatement()
	{
		m_stmt = null;
		m_bReady = false;
	}

	void close()
	{
		if (m_stmt != null)
		{
			try
			{
				m_stmt.close();
			}
			catch (Throwable throwable)
			{
			}
			retire();
			m_stmt = null;
		}
	}

	void create(Connection conn) throws Exception
	{
		m_stmt = conn.createStatement();
		m_bReady = false;
	}

	void retire()
	{
		m_bReady = true;
	}
}
