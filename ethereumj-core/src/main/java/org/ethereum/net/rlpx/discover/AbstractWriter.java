package org.ethereum.net.rlpx.discover;

import java.io.IOException;

public abstract class AbstractWriter extends Thread {
    private static final int DELAY = 60 * 1000; // one minute

    protected Crawler master;

    protected abstract void write() throws IOException;

    public AbstractWriter(Crawler master) {
        this.master = master;
    }

    @Override
    public void run() {
        while(master.isActive()) {
            try {
                Thread.sleep(DELAY);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            try {
                write();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
    }
}
