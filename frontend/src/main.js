import Vue from 'vue';
import VueSocketIOExt from 'vue-socket.io-extended';
import io from 'socket.io-client';
import { BootstrapVue, IconsPlugin } from 'bootstrap-vue';
import VJsoneditor from 'v-jsoneditor/src/index';
import 'bootstrap/dist/css/bootstrap.css';
import 'bootstrap-vue/dist/bootstrap-vue.css';
import App from './App';
import router from './router';
import store from './store';


Vue.config.productionTip = false;

// set up Vue socket.io-extended
const socket = io('http://localhost:5000', { origins: 'localhost:8080' });
Vue.use(VueSocketIOExt, socket, { store });

// Install BootstrapVue
Vue.use(BootstrapVue);
// Optionally install the BootstrapVue icon components plugin
Vue.use(IconsPlugin);
// install jsoneditor
Vue.use(VJsoneditor);

/* eslint-disable no-new */
Window.app = new Vue({
  el: '#app',
  store,
  router,
  render: h => h(App),

  sockets: {
    connect() {
      // eslint-disable-next-line no-console
      console.log('socket connected');
    },
    customEmit(val) {
      // eslint-disable-next-line no-console
      console.log('this method was fired by the socket server. eg: io.emit("customEmit", data)');
      // eslint-disable-next-line no-console
      console.log(val);
    },
  },
  methods: {
    clickButton(val) {
      // this.$socket.client is `socket.io-client` instance
      this.$socket.client.emit('emit_method', val);
    },
  },
});
