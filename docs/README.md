<p align="center">
  <img src="assets/banner.svg" alt="effect-harness" width="100%">
</p>

# Documentation

New here? Start with **[Getting started](getting-started.md)**, then read **[Concepts](concepts.md)**. The rest can be read in any order.

|     | Guide                                 | Read it when you want to…                                                   |
| :-: | ------------------------------------- | --------------------------------------------------------------------------- |
| 🚀  | [Getting started](getting-started.md) | run a first conversation, then add a real model and storage                 |
| 🧭  | [Concepts](concepts.md)               | understand loops, clients, runs, entries, tasks and documents               |
| 💬  | [Conversations](conversations.md)     | submit, steer, fork, reset, compact and stream live events                  |
| 🛠️  | [Tools](tools.md)                     | declare tools, make them crash-safe, return images, use the stock tools     |
| 🧩  | [Extensions](extensions.md)           | shape the system prompt, hook into the loop, mix and match per conversation |
| 🤖  | [Providers & accounts](providers.md)  | connect OpenAI, Anthropic, Codex or Claude, or let users add accounts       |
| 💾  | [Persistence](persistence.md)         | pick a storage backend, know what recovery covers, keep your own state      |
| 🌍  | [Deployment](deployment.md)           | split client and loop, serve WebSockets, run on Cloudflare                  |
| 📚  | [Reference](reference.md)             | look up a subpath, a setting or a built-in name                             |
| 📦  | [Releasing](releasing.md)             | publish a new version (maintainers)                                         |

Every module also documents itself: the comment at the top of each file under [`src/`](../src) explains what it is for and which part of [Pi](https://github.com/earendil-works/pi) it ports.
