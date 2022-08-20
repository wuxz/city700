package com.bode.city700.integrition;

import java.io.Serializable;

public class Bytes implements Serializable
{

	/**
	 * 
	 */
	private static final long serialVersionUID = 4057881468252948810L;

	byte m_arData[];

	public Bytes()
	{
		m_arData = null;
	}

	public Bytes(byte arData[])
	{
		m_arData = null;
		m_arData = arData;
	}

	public byte[] getBytes()
	{
		return m_arData;
	}
}
