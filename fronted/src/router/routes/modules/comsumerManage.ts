import type { AppRouteModule } from '/@/router/types';

import { LAYOUT } from '/@/router/constant';
import { t } from '/@/hooks/web/useI18n';

const comsumerManage: AppRouteModule = {
  path: '/comsumer',
  name: 'comsumerManage',
  component: LAYOUT,
  redirect: '/comsumer/comsumerManage',
  meta: {
    orderNo: 2,
    icon: 'ant-design:bars-outlined',
    title: t('routes.manage.comsumer.comsumerManage'),
  },
  children: [
    {
      path: 'comsumerManage',
      name: 'ComsumerManage',
      component: () => import('/@/views/manage/comsumerManage/index.vue'),
      meta: {
        // affix: true,
        title: t('routes.manage.comsumer.comsumerPage'),
      },
    },
  ],
};

export default comsumerManage;
