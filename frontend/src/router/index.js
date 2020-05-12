import Vue from 'vue';
import Router from 'vue-router';

const routerOptions = [
  { path: '/', component: 'About' },
  { path: '/database', component: 'Database' },
  { path: '/mapping', component: 'Mapping' },
  { path: '/engine', component: 'Engine' },
  { path: '/about', component: 'About' },
  { path: '*', component: 'NotFound' },
];

const routes = routerOptions.map(
  route => ({
    ...route,
    component: () => import(`@/components/${route.component}.vue`),
  }),
);

Vue.use(Router);

export default new Router({
  routes,
  mode: 'history',
});

