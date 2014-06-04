package com.netflix.dyno.connectionpool.impl.utils;

/**
 * A simple rate limiter based on <a href="http://lucene.apache.org/">Lucene</a>'s RateLimiter.SimpleRateLimiter.
 */
public class RateLimiterUtil {

    private volatile int msgPerSec;
    private volatile double nsPerMsg;
    private volatile long lastNS;

    public static RateLimiterUtil create(int msgPerSec) {
    	return new RateLimiterUtil(msgPerSec);
    }
    
    public RateLimiterUtil(int msgPerSec) {
        setMsgPerSec(msgPerSec);
    }

    public void setMsgPerSec(int msgPerSec) {
        this.msgPerSec = msgPerSec;
        nsPerMsg = 1000000000. / msgPerSec;

    }

    public int getMsgPerSec() {
        return this.msgPerSec;
    }
    
    public void acquire() throws InterruptedException {
    	pause(1);
    }

    /** Pauses, if necessary, to keep the instantaneous IO
     *  rate at or below the target. NOTE: multiple threads
     *  may safely use this, however the implementation is
     *  not perfectly thread safe but likely in practice this
     *  is harmless (just means in some rare cases the rate
     *  might exceed the target).  It's best to call this
     *  with a biggish count, not one byte at a time.
     *  @return the pause time in nano seconds
     * */
    public long pause(int msgs) throws InterruptedException {
        if (msgs == 1) {
            return 0;
        }

        // TODO: this is purely instantaneous rate; maybe we
        // should also offer decayed recent history one?
        final long targetNS = lastNS = lastNS + ((long) (msgs * nsPerMsg));
        final long startNS;
        long curNS = startNS = System.nanoTime();
        if (lastNS < curNS) {
            lastNS = curNS;
        }

        // While loop because Thread.sleep doesn't always sleep
        // enough:
        while(true) {
            final long pauseNS = targetNS - curNS;
            if (pauseNS > 0) {
                Thread.sleep((int) (pauseNS/1000000), (int) (pauseNS % 1000000));
                curNS = System.nanoTime();
                continue;
            }
            break;
        }
        return curNS - startNS;
    }
}
