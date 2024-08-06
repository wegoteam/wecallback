import { $t } from "@/plugins/i18n";

export default {
  path: "/broker",
  redirect: "/brokerManage",
  meta: {
    icon: "ant-design:cluster-outlined",
    // showLink: false,
    title: $t("menus.brokerManage"),
    rank: 1
  },
  children: [
    {
      path: "/brokerManage",
      name: "节点管理",
      component: () => import("@/views/brokerManage/index.vue"),
      meta: {
        title: $t("menus.brokerManage")
      }
    }
  ]
} satisfies RouteConfigsTable;
