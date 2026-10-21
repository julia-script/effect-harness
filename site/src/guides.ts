/**
 * The guides, read straight from `docs/`, so GitHub and the site show the same text. Each comes
 * with its code blocks, highlighted at build time.
 */
import * as Markdown from '@foldkit/markdown'
import type { Option } from 'effect'
import { Array } from 'effect'
import concepts from '../../docs/concepts.md'
import conceptsCode from '../../docs/concepts.md?code'
import conversations from '../../docs/conversations.md'
import conversationsCode from '../../docs/conversations.md?code'
import deployment from '../../docs/deployment.md'
import deploymentCode from '../../docs/deployment.md?code'
import extensions from '../../docs/extensions.md'
import extensionsCode from '../../docs/extensions.md?code'
import gettingStarted from '../../docs/getting-started.md'
import gettingStartedCode from '../../docs/getting-started.md?code'
import persistence from '../../docs/persistence.md'
import persistenceCode from '../../docs/persistence.md?code'
import providers from '../../docs/providers.md'
import providersCode from '../../docs/providers.md?code'
import reference from '../../docs/reference.md'
import referenceCode from '../../docs/reference.md?code'
import releasing from '../../docs/releasing.md'
import releasingCode from '../../docs/releasing.md?code'
import tools from '../../docs/tools.md'
import toolsCode from '../../docs/tools.md?code'

export interface Guide {
  readonly slug: string
  readonly icon: string
  readonly title: string
  /** When to read it. */
  readonly blurb: string
  readonly document: Markdown.MarkdownDocument
  readonly code: ReadonlyArray<string | null>
}

const guide = (
  slug: string,
  icon: string,
  title: string,
  blurb: string,
  wire: unknown,
  code: ReadonlyArray<string | null>,
): Guide => ({ slug, icon, title, blurb, document: Markdown.decodeDocument(wire), code })

export const guides: ReadonlyArray<Guide> = [
  guide(
    'getting-started',
    '🚀',
    'Getting started',
    'Run a first conversation, then add a real model and storage.',
    gettingStarted,
    gettingStartedCode,
  ),
  guide(
    'concepts',
    '🧭',
    'Concepts',
    'Understand loops, clients, runs, entries, tasks and documents.',
    concepts,
    conceptsCode,
  ),
  guide(
    'conversations',
    '💬',
    'Conversations',
    'Submit, steer, fork, reset, compact and stream live events.',
    conversations,
    conversationsCode,
  ),
  guide(
    'tools',
    '🛠️',
    'Tools',
    'Declare tools, make them crash-safe, return images, use the stock tools.',
    tools,
    toolsCode,
  ),
  guide(
    'extensions',
    '🧩',
    'Extensions',
    'Shape the system prompt, hook into the loop, mix and match per conversation.',
    extensions,
    extensionsCode,
  ),
  guide(
    'providers',
    '🤖',
    'Providers & accounts',
    'Connect OpenAI, Anthropic, Codex or Claude, or let users add accounts.',
    providers,
    providersCode,
  ),
  guide(
    'persistence',
    '💾',
    'Persistence',
    'Pick a storage backend, know what recovery covers, keep your own state.',
    persistence,
    persistenceCode,
  ),
  guide(
    'deployment',
    '🌍',
    'Deployment',
    'Split client and loop, serve WebSockets, run on Cloudflare.',
    deployment,
    deploymentCode,
  ),
  guide(
    'reference',
    '📚',
    'Reference',
    'Look up a subpath, a setting or a built-in name.',
    reference,
    referenceCode,
  ),
  guide(
    'releasing',
    '📦',
    'Releasing',
    'Publish a new version (maintainers).',
    releasing,
    releasingCode,
  ),
]

export const find = (slug: string): Option.Option<Guide> =>
  Array.findFirst(guides, (guide) => guide.slug === slug)
