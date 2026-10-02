/**
 * Error classification drives the retry policy:
 *  - PermanentError: retrying cannot help (invalid payload, business rule violation, unsupported version).
 *    The message goes straight to the dead-letter queue.
 *  - Anything else is treated as transient (database or provider timeout) and retried with delays.
 */
export class PermanentError extends Error {
  constructor(message: string, options?: { cause?: unknown }) {
    super(message, options);
    this.name = "PermanentError";
  }
}

export const isPermanent = (error: unknown): boolean =>
  error instanceof PermanentError ||
  (error instanceof Error && (error.name === "ZodError" || error.name === "UnsupportedVersionError"));
