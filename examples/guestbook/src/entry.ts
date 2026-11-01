import { Runtime } from 'foldkit'
import { layerClient } from 'foldkit-serverfn/client'

import { Flags, Message, Model, init, subscriptions, update, view } from './main'
import { GuestbookFunctions } from './serverFunction'
import './styles.css'

const application = Runtime.makeApplication({
  Model,
  Flags,
  init,
  update,
  view,
  subscriptions,
  resources: layerClient(GuestbookFunctions),
  container: document.getElementById('root'),
  devTools: {
    Message,
  },
})

Runtime.hydrate(application)
