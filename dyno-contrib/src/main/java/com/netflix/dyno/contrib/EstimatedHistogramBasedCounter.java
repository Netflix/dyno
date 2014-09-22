package com.netflix.dyno.contrib;

import com.google.common.base.Objects;
import com.netflix.dyno.connectionpool.impl.utils.EstimatedHistogram;
import com.netflix.servo.annotations.DataSourceType;
import com.netflix.servo.monitor.AbstractMonitor;
import com.netflix.servo.monitor.MonitorConfig;

public abstract class EstimatedHistogramBasedCounter extends AbstractMonitor<Number> {

	protected final EstimatedHistogram estHistogram; 
	
	/**
	 * Creates a new instance of the counter.
	 */
	public EstimatedHistogramBasedCounter(final String name, final EstimatedHistogram histogram) {
		super(MonitorConfig.builder(name).build().withAdditionalTag(DataSourceType.COUNTER));
		this.estHistogram = histogram;
	}

	/** {@inheritDoc} */
	@Override
	public boolean equals(Object obj) {
		if (obj == null || !(obj instanceof EstimatedHistogramBasedCounter)) {
			return false;
		}
		EstimatedHistogramBasedCounter m = (EstimatedHistogramBasedCounter) obj;
		return config.equals(m.getConfig()) && estHistogram.equals(m.estHistogram);
	}

	/** {@inheritDoc} */
	@Override
	public int hashCode() {
		return Objects.hashCode(config, estHistogram.hashCode());
	}

	/** {@inheritDoc} */
	@Override
	public String toString() {
		return Objects.toStringHelper(this)
				.add("config", config)
				.add("count", getValue())
				.toString();
	}
	
	public static class EstimatedHistogramMean extends EstimatedHistogramBasedCounter {

		public EstimatedHistogramMean(String name, EstimatedHistogram histogram) {
			super(name, histogram);
		}

		@Override
		public Number getValue() {
			return estHistogram.mean();
		}
	}

	public static class EstimatedHistogramPercentile extends EstimatedHistogramBasedCounter {

		private final double percentile;
		
		public EstimatedHistogramPercentile(String name, EstimatedHistogram histogram, double pVal) {
			super(name, histogram);
			percentile = pVal;
		}

		@Override
		public Number getValue() {
			return estHistogram.percentile(percentile);
		}
	}
}
