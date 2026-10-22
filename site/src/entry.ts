import { Runtime } from 'foldkit'
import type { Services } from './main.ts'
import { init, managedResources, Message, Model, subscriptions, update, view } from './main.ts'
import './styles.css'

const application = Runtime.makeApplication<Model, Message, never, Services>({
  Model,
  init,
  update,
  view,
  subscriptions,
  managedResources,
  container: document.getElementById('root'),
  routing: {
    onUrlRequest: (request) => Message.ClickedLink({ request }),
    onUrlChange: (url) => Message.ChangedUrl({ url }),
  },
  devTools: { Message },
})

Runtime.run(application)
