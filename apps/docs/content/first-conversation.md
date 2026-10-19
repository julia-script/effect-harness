# Your first conversation

Build a small application that submits an input, reads a stored answer, and retries the input without creating another submission. Its model runs offline, so the result is repeatable.

This lesson assumes TypeScript and basic `Effect`, `Layer`, and `Schema` knowledge. Start in an empty directory with Bun 1.4.2. No API credentials are needed.

## 1. Create the project

Run these commands in your terminal:

```sh
mkdir hello-harness
cd hello-harness
bun init -y
bun add effect@4.0.1 effect-harness@0.5.0
```

The directory now contains a package manifest and installed dependencies. Keep the generated files; we will add one program beside them.

## 2. Add the program

Create `first.ts` and copy this complete program into it. The fixed model response is part of the lesson; it lets us focus on the conversation API.

<!-- example:first -->

## 3. Run the conversation

```sh
bun first.ts
```

The output is:

```text
Hello, harness!
Same submission: true
```

Notice that the answer is read from a committed entry identified by `InputDone.answer`. The second submission uses the same `requestId`, and its ID matches the first.

## 4. Change the request identity

In the final `Conversation.submit` call, replace `input` with `{ ...input, requestId: 'hello-2' }`. Run the file again.

The last line now reads `Same submission: false`. You have created a distinct admission by changing its request identity. Both submissions in each run belong to the same conversation.

## Next steps

The memory store exists only for this run. Read [how to preserve conversations across restarts](/guides/persistence) before relying on history between launches. Explore tool execution with [how to add a tool](/guides/tools), or control a model yourself in the [conversation lab](/lab).
