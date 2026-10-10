---
'effect-harness': minor
---

Add ToolkitFileSystem and ToolkitProcess Layers that bind native Effect filesystem, path and child process services to an explicit backend root. Preserve scoped cleanup, backend errors, literal argv and caller cwd/environment overrides. Support an explicit lexical root policy; this is not a security sandbox. Canonical per-toolkit registry invocation selection remains a separate integration dependency.
