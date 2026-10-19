/**
 * Context overflow, recognized from a provider's error text; ported from pi-ai's
 * `utils/overflow.ts`.
 */

const OVERFLOW_PATTERNS = [
  // Anthropic and z.ai token overflow
  /prompt (?:is )?too long/iu,
  // z.ai CN endpoint token overflow
  /prompt exceeds max length/iu,
  // Anthropic request byte-size overflow (HTTP 413)
  /request_too_large/iu,
  // Amazon Bedrock
  /input is too long for requested model/iu,
  // OpenAI (Completions & Responses API)
  /exceeds the context window/iu,
  // OpenAI-compatible proxies (LiteLLM)
  /exceeds (?:the )?(?:model'?s )?maximum context length(?: of [\d,]+ tokens?|\s*\([\d,]+\))/iu,
  // Google (Gemini)
  /input token count.*exceeds the maximum/iu,
  // xAI (Grok)
  /maximum prompt length is \d+/iu,
  // Groq
  /reduce the length of the messages/iu,
  // OpenRouter (most backends)
  /maximum context length is \d+ tokens/iu,
  // OpenRouter/Poolside
  /exceeds (?:the )?maximum allowed input length of [\d,]+ tokens?/iu,
  // Together AI
  /input \(\d+ tokens\) is longer than the model'?s context length \(\d+ tokens\)/iu,
  // GitHub Copilot
  /exceeds the limit of \d+/iu,
  // llama.cpp server
  /exceeds the available context size/iu,
  // LM Studio
  /greater than the context length/iu,
  // MiniMax
  /context window exceeds limit/iu,
  // Kimi For Coding
  /exceeded model token limit/iu,
  // Mistral
  /too large for model with \d+ maximum context length/iu,
  // DS4 server
  /prompt has [\d,]+ tokens?, but the configured context size is [\d,]+ tokens?/iu,
  // z.ai non-standard finish_reason surfaced as error text
  /model_context_window_exceeded/iu,
  // Ollama explicit overflow error
  /prompt too long; exceeded (?:max )?context length/iu,
  // DashScope / Qwen Token Plan
  /range of input length should be/iu,
  // Generic fallback
  /context[_ ]length[_ ]exceeded/iu,
  // Generic fallback
  /too many tokens/iu,
  // Generic fallback
  /token limit exceeded/iu,
]

const NON_OVERFLOW_PATTERNS = [
  // AWS Bedrock non-overflow errors
  /^(Throttling error|Service unavailable):/iu,
  // Generic rate limiting
  /rate limit/iu,
  // Generic HTTP 429 style
  /too many requests/iu,
]

export const isOverflowText = (text: string): boolean =>
  !NON_OVERFLOW_PATTERNS.some((pattern) => pattern.test(text)) &&
  OVERFLOW_PATTERNS.some((pattern) => pattern.test(text))
