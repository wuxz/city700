package com.bode.city700.integrition;

import java.beans.Beans;

class LogData extends Beans
{

	public static final int TYPE_BYTES = 4;

	public static final int TYPE_DATE = 2;

	public static final int TYPE_INT = 0;

	public static final int TYPE_STRING = 1;

	public static final int TYPE_TIMESTAMP = 3;

	public static final int TYPE_UNKNOWN = -1;

	Object m_arData[];

	int m_arParamType[];

	boolean m_bExecuted;

	LogData()
	{
		m_arParamType = null;
		m_bExecuted = false;
		m_arData = null;
	}

	public String getParamTypeString()
	{
		if (m_arParamType == null)
		{
			return null;
		}
		String strRet = "";
		for (int i = 0; i < m_arParamType.length; i++)
		{
			strRet = strRet + m_arParamType[i] + "^";
		}

		return strRet;
	}

	public void setParamCount(int nParams)
	{
		if (nParams <= 0)
		{
			return;
		}
		m_arParamType = new int[nParams];
		m_arData = new Object[nParams];
		for (int i = 0; i < nParams; i++)
		{
			m_arParamType[i] = -1;
			m_arData[i] = null;
		}

	}
}
