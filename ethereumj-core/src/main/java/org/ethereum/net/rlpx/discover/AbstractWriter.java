package org.ethereum.net.rlpx.discover;

import java.io.IOException;

public abstract class AbstractWriter extends Thread {
    private static final int DELAY = 60 * 1000; // one minute

    protected Crawler master;

    protected abstract void write() throws IOException;

    public AbstractWriter(Crawler master) {
		master.logger.info("" + master);
        this.master = master;
    }

    @Override
    public void run() {
		master.logger.info("Activity: " + master.active);
        while(master.active) {
			master.logger.info("BBBBBBB");
            try {
                Thread.sleep(DELAY);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            try {
				master.logger.info("Writing to files");
                write();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
    }
}
