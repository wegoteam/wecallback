import type { AppRouteModule } from '/@/router/types';

import { LAYOUT } from '/@/router/constant';
import { t } from '/@/hooks/web/useI18n';

const appManage: AppRouteModule = {
  path: '/app',
  name: 'appManage',
  component: LAYOUT,
  redirect: '/app/appManage',
  meta: {
    orderNo: 4,
    icon: 'ant-design:appstore-twotone',
    title: t('routes.manage.app.appManage'),
  },
  children: [
    {
      path: 'appManage',
      name: 'AppManage',
      component: () => import('/@/views/manage/appManage/index.vue'),
      meta: {
        // affix: true,
        title: t('routes.manage.app.appPage'),
      },
    },
  ],
};

export default appManage;
