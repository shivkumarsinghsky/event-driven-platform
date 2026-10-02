import { isPermanent } from "./errors.js";

export type FailureDecision =
  | { action: "retry"; attempt: number; delayMs: number; queue: string }
  | { action: "dead-letter"; reason: "permanent-error" | "retries-exhausted" };

/**
 * Pure decision function: given how many times a message has already been attempted and the error,
 * decide whether to schedule a delayed retry or dead-letter it. Kept pure so it is exhaustively unit-tested.
 *
 * @param attemptsSoFar number of failed attempts including this one (1 after the first failure)
 */
export function decideOnFailure(
  queue: string,
  attemptsSoFar: number,
  error: unknown,
  retryDelaysMs: number[],
): FailureDecision {
  if (isPermanent(error)) return { action: "dead-letter", reason: "permanent-error" };
  const delayMs = retryDelaysMs[attemptsSoFar - 1];
  if (delayMs === undefined) return { action: "dead-letter", reason: "retries-exhausted" };
  return { action: "retry", attempt: attemptsSoFar, delayMs, queue: retryQueueName(queue, delayMs) };
}

export const retryQueueName = (queue: string, delayMs: number) => `${queue}.retry.${delayMs}ms`;
export const deadLetterQueueName = (queue: string) => `${queue}.dlq`;
