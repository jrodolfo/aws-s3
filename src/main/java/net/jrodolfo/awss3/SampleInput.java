package net.jrodolfo.awss3;

/**
 * Resolves sample inputs from positional arguments and environment variables.
 */
public final class SampleInput {

    private SampleInput() {
    }

    /**
     * Resolves a required value from the command line or the environment.
     */
    public static String required(String[] args, int index, String envName, String description) {
        String value = optional(args, index, envName, null);
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException("Missing " + description + ". Provide argument " + index
                    + " or set environment variable " + envName + ".");
        }
        return value.trim();
    }

    /**
     * Resolves an optional value from the command line or the environment.
     */
    public static String optional(String[] args, int index, String envName, String defaultValue) {
        if (args.length > index && args[index] != null && !args[index].trim().isEmpty()) {
            return args[index].trim();
        }

        String envValue = System.getenv(envName);
        if (envValue != null && !envValue.trim().isEmpty()) {
            return envValue.trim();
        }

        return defaultValue;
    }

    /**
     * Resolves an optional boolean value from the command line or the environment.
     */
    public static boolean optionalBoolean(String[] args, int index, String envName, boolean defaultValue) {
        String value = optional(args, index, envName, null);
        if (value == null) {
            return defaultValue;
        }
        return Boolean.parseBoolean(value);
    }
}
