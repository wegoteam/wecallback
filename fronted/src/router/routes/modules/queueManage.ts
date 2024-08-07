import type { AppRouteModule } from '/@/router/types';

import { LAYOUT } from '/@/router/constant';
import { t } from '/@/hooks/web/useI18n';

const queueManage: AppRouteModule = {
  path: '/queue',
  name: 'queueManage',
  component: LAYOUT,
  redirect: '/queue/queueManage',
  meta: {
    orderNo: 3,
    icon: 'ion:grid-outline',
    title: t('routes.manage.queue.queueManage'),
  },
  children: [
    {
      path: 'queueManage',
      name: 'queueManage',
      component: () => import('/@/views/manage/queueManage/index.vue'),
      meta: {
        // affix: true,
        title: t('routes.manage.queue.queuePage'),
      },
    },
  ],
};

export default queueManage;
