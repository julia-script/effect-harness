/**
 * Test support: a Responses API stream that answers `Hello`.
 */

const response = (status: string) => ({
  id: 'resp_1',
  object: 'response',
  created_at: 1,
  model: 'gpt-5',
  status,
  output: [],
  metadata: null,
  temperature: null,
  top_p: null,
  tools: [],
  tool_choice: 'auto',
  error: null,
  incomplete_details: null,
  instructions: null,
  parallel_tool_calls: false,
})

const message = (status: string, text: string) => ({
  type: 'message',
  id: 'msg_1',
  role: 'assistant',
  status,
  content: text === '' ? [] : [{ type: 'output_text', text, annotations: [] }],
})

/** The events of a response that answers `Hello`. */
export const events = [
  { type: 'response.created', sequence_number: 1, response: response('in_progress') },
  {
    type: 'response.output_item.added',
    sequence_number: 2,
    output_index: 0,
    item: message('in_progress', ''),
  },
  {
    type: 'response.output_text.delta',
    sequence_number: 3,
    item_id: 'msg_1',
    output_index: 0,
    content_index: 0,
    delta: 'Hello',
    logprobs: [],
  },
  {
    type: 'response.output_item.done',
    sequence_number: 4,
    output_index: 0,
    item: message('completed', 'Hello'),
  },
  {
    type: 'response.completed',
    sequence_number: 5,
    response: {
      ...response('completed'),
      usage: {
        input_tokens: 10,
        output_tokens: 20,
        total_tokens: 30,
        input_tokens_details: { cached_tokens: 0 },
        output_tokens_details: { reasoning_tokens: 0 },
      },
    },
  },
]
