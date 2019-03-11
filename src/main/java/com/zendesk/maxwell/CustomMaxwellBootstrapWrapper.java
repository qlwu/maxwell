package com.zendesk.maxwell;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;

public class CustomMaxwellBootstrapWrapper
{
	static final Logger LOGGER = LoggerFactory.getLogger(CustomMaxwellBootstrapWrapper.class);

	static final String RedisKey_ReportStatus="maxwell_running_status";
	static final String RedisKey_BootstrapLock = "maxwell_bootstrap_lock";
	static final String RedisVal_MaxwellStatusPrefix="hb_at_";

	private RedisTemplate redisTemplate;
	private MaxwellConfig config;
	CustomMaxwellBootstrapWrapper(MaxwellConfig config)
	{
		
		this.config = config;
		this.redisTemplate = new RedisTemplate(config.redisHost, config.redisPort);
	}
	
	/**
	 * 上报maxwell的状态
	 */
	void scheduledReportMaxStatus()
	{
		Thread thread = new Thread() {
			@Override
			public void run() {
				while(true) {
					try {
						Thread.sleep(1000L*10);
						redisTemplate.setValue(RedisKey_ReportStatus, RedisVal_MaxwellStatusPrefix+System.currentTimeMillis(), 60);
					}catch(Exception e) {
						e.printStackTrace();
					}
				}
			}
		};
		thread.setName("reportMaxwellStatus");
		thread.setDaemon(true);
		thread.start();

		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				LOGGER.info("receive exit signal .............");
				redisTemplate.del(RedisKey_BootstrapLock);
				redisTemplate.close();
			}
		});
	}

	/**
	 * 判断是否可以启动maxwell
	 */
	boolean isCanBootstrapMaxwell() 
	{
		String maxwellStatus = redisTemplate.getValue(RedisKey_ReportStatus);
		if( maxwellStatus!=null && maxwellStatus.startsWith(RedisVal_MaxwellStatusPrefix) )
		{
			LOGGER.info("maxwell already running,cann't bootstrap maxwell.............");
			return false;
		}
		if( redisTemplate.getLock(RedisKey_BootstrapLock, 120)==false )
		{
			LOGGER.info("getBootstrapLock Fail,cann't bootstrap maxwell..........");
			return false;
		}
		return true;
	}


	/**
	 * 轮询检测
	 */
	void detectAndWatingBootstrap() 
	{
		while(true) {
			try {
				Thread.sleep(1000L*60);
				LOGGER.info("start to detuct maxwell running status..........");
				if(isCanBootstrapMaxwell()==true)
					break;
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		LOGGER.info("start to bootstrap maxwell from detect...............");
		Maxwell.startUpMaxwell(config,this);
	}


	static class RedisTemplate{
		private Jedis jedis;

		RedisTemplate(String host,int port){
			this.jedis = new Jedis(host, port);
			jedis.connect();
		}

		public void setValue(String key,String value,int seconds) {
			jedis.setex(key, seconds, value);
		}

		public String getValue(String key) {
			return jedis.get(key);
		}

		public boolean getLock(String key,int seconds) {
			long result = jedis.setnx(key, "lock");
			LOGGER.info("debug jedis.setnx result is "+result);
			if(result==1) {//设置启动过期时间
				jedis.expire(key, seconds);
				return true;
			}
			return false;
		}
		
		public void del(String key) {
			jedis.del(key);
		}

		public void close() {
			this.jedis.close();
		}
	}

}
