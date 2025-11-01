package io.synapsedb.index.mapping;

import java.util.Objects;

/**
 * Dynamic mapping policy.
 * ENABLED: allow unknown fields to be added.
 * DISABLED: do not add unknown fields (caller decides to ignore).
 * STRICT: reject unknown fields.
 */
public final class DynamicMapping {

    public enum Mode { ENABLED, DISABLED, STRICT }

    private final Mode mode;

    private DynamicMapping(Mode mode) {
        this.mode = Objects.requireNonNull(mode, "mode");
    }

    public static DynamicMapping enabled() { return new DynamicMapping(Mode.ENABLED); }
    public static DynamicMapping disabled() { return new DynamicMapping(Mode.DISABLED); }
    public static DynamicMapping strict() { return new DynamicMapping(Mode.STRICT); }

    public Mode mode() { return mode; }

    public boolean allowUnknownFields() {
        return mode == Mode.ENABLED;
    }

    public boolean rejectUnknownFields() {
        return mode == Mode.STRICT;
    }

    @Override
    public String toString() { return "DynamicMapping{" + mode + "}"; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DynamicMapping)) return false;
        return mode == ((DynamicMapping) o).mode;
    }

    @Override
    public int hashCode() { return mode.hashCode(); }
}
