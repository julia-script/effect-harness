import { Layer } from 'effect'
import { Runtime } from 'foldkit'
import { Lab } from './Lab'
import { Message, Model, init, subscriptions, update, view } from './Main'
import './style.css'

Runtime.run(
  Runtime.makeApplication({
    Model,
    init,
    update,
    view,
    subscriptions,
    resources: Layer.orDie(Lab.layer),
    routing: {
      onUrlRequest: (request) => Message.RequestedUrl({ request }),
      onUrlChange: (url) => Message.ChangedUrl({ url }),
    },
    container: document.getElementById('root'),
  }),
)
