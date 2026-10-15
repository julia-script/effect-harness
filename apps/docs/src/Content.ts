import tools from '../content/tools.md?raw'
import providers from '../../../docs/providers.md?raw'
import persistence from '../content/persistence.md?raw'
import observations from '../content/observation.md?raw'
import composition from '../../../docs/explanation/composition.md?raw'
import recovery from '../../../docs/explanation/recovery.md?raw'
import example from '../../example/README.md?raw'
import recoverySource from '../../example/src/tour/Recovery.ts?raw'
import forksSource from '../../example/src/tour/Forks.ts?raw'
import documentsSource from '../../example/src/tour/Documents.ts?raw'
import portableSource from '../../example/src/Portable.ts?raw'

import tutorial from '../content/first-conversation.md?raw'
import submissionReference from '../content/submissions.md?raw'
import storageReference from '../content/storage.md?raw'
import firstSource from './examples/FirstConversation.ts?raw'
import toolsSource from './examples/Tools.ts?raw'
import toolImagesSource from './examples/ToolImages.ts?raw'
import observeSource from './examples/Observe.ts?raw'

const embed = (body: string, name: string, source: string) =>
  body.replace(`<!-- example:${name} -->`, `\n\`\`\`ts\n${source}\n\`\`\`\n`)

export interface Page {
  readonly path: string
  readonly title: string
  readonly group: string
  readonly summary: string
  readonly body: string
  readonly source: string
}
export const pages: ReadonlyArray<Page> = [
  {
    path: '/tutorials/first-conversation',
    title: 'Your first conversation',
    group: 'Tutorials',
    summary: 'Submit an input, read its answer, and retry it without a duplicate.',
    body: embed(tutorial, 'first', firstSource),
    source: 'apps/docs/content/first-conversation.md',
  },
  {
    path: '/reference/submissions',
    title: 'Submissions',
    group: 'Reference',
    summary: 'Draft fields, settlement records, and client operations.',
    body: submissionReference,
    source: 'apps/docs/content/submissions.md',
  },
  {
    path: '/reference/storage',
    title: 'Storage layers',
    group: 'Reference',
    summary: 'Built-in stores, required services, and ownership constraints.',
    body: storageReference,
    source: 'apps/docs/content/storage.md',
  },
  {
    path: '/guides/tools',
    title: 'Tools & dependencies',
    group: 'Guides',
    summary: 'Declare a contract. Supply its implementation through a Layer.',
    body: embed(embed(tools, 'tools', toolsSource), 'tool-images', toolImagesSource),
    source: 'apps/docs/content/tools.md',
  },
  {
    path: '/guides/providers',
    title: 'Connect a model',
    group: 'Guides',
    summary: 'Native Effect AI providers and explicit model descriptors.',
    body: providers,
    source: 'docs/providers.md',
  },
  {
    path: '/guides/persistence',
    title: 'Persist application state',
    group: 'Guides',
    summary: 'Storage, schema-backed documents, and atomic transactions.',
    body: embed(persistence, 'recovery', recoverySource),
    source: 'apps/docs/content/persistence.md',
  },
  {
    path: '/guides/observation',
    title: 'Observe committed work',
    group: 'Guides',
    summary: 'Read entries, watch documents, and wait for submissions.',
    body: embed(observations, 'observe', observeSource),
    source: 'apps/docs/content/observation.md',
  },
  {
    path: '/explanation/composition',
    title: 'Client & runtime',
    group: 'Explanation',
    summary: 'How the client, backend, and local runtime fit together.',
    body: composition,
    source: 'docs/explanation/composition.md',
  },
  {
    path: '/explanation/recovery',
    title: 'Restart & recovery',
    group: 'Explanation',
    summary: 'What survives a restart, what repeats, and what cannot be guaranteed.',
    body: recovery,
    source: 'docs/explanation/recovery.md',
  },
  {
    path: '/examples',
    title: 'Runnable examples',
    group: 'Examples',
    summary: 'Complete programs using the public package imports.',
    body: example,
    source: 'apps/example/README.md',
  },
  {
    path: '/examples/recovery',
    title: 'Recover a submission',
    group: 'Examples',
    summary: 'A SQLite-backed application that can reopen the same request.',
    body: `# Recover a submission\n\nComplete source from the example application.\n\n\`\`\`ts\n${recoverySource}\n\`\`\``,
    source: 'apps/example/src/tour/Recovery.ts',
  },
  {
    path: '/examples/forks',
    title: 'Fork a conversation',
    group: 'Examples',
    summary: 'Branch from a committed answer and run both conversations.',
    body: `# Fork a conversation\n\nComplete source from the example application.\n\n\`\`\`ts\n${forksSource}\n\`\`\``,
    source: 'apps/example/src/tour/Forks.ts',
  },
  {
    path: '/examples/documents',
    title: 'Commit a document',
    group: 'Examples',
    summary: 'Update application state and append an entry in one transaction.',
    body: `# Commit a document\n\nComplete source from the example application.\n\n\`\`\`ts\n${documentsSource}\n\`\`\``,
    source: 'apps/example/src/tour/Documents.ts',
  },
  {
    path: '/examples/portable',
    title: 'Run in the browser',
    group: 'Examples',
    summary: 'The same public API works with platform-free storage and an offline model.',
    body: `# Run in the browser\n\nComplete source from the example application.\n\n\`\`\`ts\n${portableSource}\n\`\`\``,
    source: 'apps/example/src/Portable.ts',
  },
]
export const github = 'https://github.com/julia-script/effect-harness'
export const sourceUrl = (source: string) =>
  source.startsWith('apps/docs/')
    ? `/sources/${source.slice('apps/docs/'.length)}.txt`
    : `${github}/blob/main/${source}`

export const resolveLink = (href: string, source: string): string => {
  if (!href || href.startsWith('#') || href.startsWith('/')) return href
  if (/^https?:/.test(href)) return href
  if (/^[a-z][a-z0-9+.-]*:/i.test(href)) return '#'
  const resolved = new URL(href, `https://docs.local/${source}`)
  const file = resolved.pathname.slice(1)
  const page = pages.find((item) => item.source === file)
  return page ? page.path + resolved.hash : sourceUrl(file) + resolved.hash
}
