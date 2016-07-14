package org.chronos.common.test;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.junit.AssumptionViolatedException;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

class ChronosTestWatcher extends TestWatcher {

	private Long startTime;

	@Override
	protected void starting(final Description description) {
		super.starting(description);
		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		Date date = new Date();
		this.startTime = date.getTime();
		String startTime = dateFormat.format(date);
		StringBuilder msg = new StringBuilder();
		msg.append("\n\n");
		msg.append("================================================================================\n");
		msg.append(" TEST START                                                                     \n");
		msg.append("--------------------------------------------------------------------------------\n");
		msg.append("    Test class:  " + description.getTestClass().getSimpleName() + "\n");
		msg.append("    Test method: " + description.getMethodName() + "\n");
		msg.append("    Start time:  " + startTime + "\n");
		msg.append("================================================================================\n");
		msg.append("\n\n");
		System.out.println(msg.toString());
	}

	@Override
	protected void failed(final Throwable e, final Description description) {
		super.failed(e, description);
		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		Date date = new Date();
		long duration = date.getTime() - this.startTime;
		this.startTime = null;
		String endTime = dateFormat.format(date);
		StringBuilder msg = new StringBuilder();
		msg.append("\n\n");
		msg.append("================================================================================\n");
		msg.append(" TEST FAILED                                                                    \n");
		msg.append("--------------------------------------------------------------------------------\n");
		msg.append("    Test class:  " + description.getTestClass().getSimpleName() + "\n");
		msg.append("    Test method: " + description.getMethodName() + "\n");
		msg.append("    End time:    " + endTime + " (Duration: " + duration + "ms)\n");
		msg.append("================================================================================\n");
		msg.append("\n\n");
		System.out.println(msg.toString());
	}

	@Override
	protected void succeeded(final Description description) {
		super.succeeded(description);
		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		Date date = new Date();
		long duration = date.getTime() - this.startTime;
		this.startTime = null;
		String endTime = dateFormat.format(date);
		StringBuilder msg = new StringBuilder();
		msg.append("\n\n");
		msg.append("================================================================================\n");
		msg.append(" TEST SUCCEEDED                                                                 \n");
		msg.append("--------------------------------------------------------------------------------\n");
		msg.append("    Test class:  " + description.getTestClass().getSimpleName() + "\n");
		msg.append("    Test method: " + description.getMethodName() + "\n");
		msg.append("    End time:    " + endTime + " (Duration: " + duration + "ms)\n");
		msg.append("================================================================================\n");
		msg.append("\n\n");
		System.out.println(msg.toString());
	}

	@Override
	protected void skipped(final AssumptionViolatedException e, final Description description) {
		super.skipped(e, description);
	}

}