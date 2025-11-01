package io.synapsedb.index.config;


import java.time.Duration;
import java.util.Objects;

/**
 * Author: Amit Tiwari
 * Date: 31/10/25
 * Configuration for index refresh behavior.
 */
public final class RefreshPolicy {

    public enum Mode {
        NONE,       // never refresh automatically
        AUTO,       // refresh periodically
        IMMEDIATE   // refresh after every write (expensive)
    }

    private final Mode mode;
    private final Duration interval; // used only for AUTO

    private RefreshPolicy(Mode mode, Duration interval) {
        this.mode = Objects.requireNonNull(mode, "mode");
        if (mode == Mode.AUTO) {
            if (interval == null || interval.isNegative() || interval.isZero()) {
                throw new IllegalArgumentException("AUTO mode requires a positive interval");
            }
            this.interval = interval;
        } else {
            this.interval = null;
        }
    }

    public static RefreshPolicy none() {
        return new RefreshPolicy(Mode.NONE, null);
    }

    public static RefreshPolicy immediate() {
        return new RefreshPolicy(Mode.IMMEDIATE, null);
    }

    public static RefreshPolicy auto(Duration interval) {
        return new RefreshPolicy(Mode.AUTO, interval);
    }

    public Mode getMode() {
        return mode;
    }

    public Duration getInterval() {
        return interval;
    }

    @Override
    public String toString() {
        return "RefreshPolicy{" +
                "mode=" + mode +
                ", interval=" + interval +
                '}';
    }
}