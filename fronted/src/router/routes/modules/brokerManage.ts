import type { AppRouteModule } from '/@/router/types';

import { LAYOUT } from '/@/router/constant';
import { t } from '/@/hooks/web/useI18n';

const brokerManage: AppRouteModule = {
  path: '/broker',
  name: 'BrokerManage',
  component: LAYOUT,
  redirect: '/broker/brokerManage',
  meta: {
    orderNo: 2,
    icon: 'ion:grid-outline',
    title: t('routes.manage.broker.brokerManage'),
  },
  children: [
    {
      path: 'brokerManage',
      name: 'BrokerManage',
      component: () => import('/@/views/manage/brokerManage/index.vue'),
      meta: {
        // affix: true,
        title: t('routes.manage.broker.brokerPage'),
      },
    },
  ],
};

export default brokerManage;
