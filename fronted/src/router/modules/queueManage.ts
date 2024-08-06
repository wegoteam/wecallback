import { $t } from "@/plugins/i18n";

export default {
  path: "/queueManage",
  redirect: "/queueManage",
  meta: {
    icon: "majesticons:box-line",
    // showLink: false,
    title: $t("menus.queueManage"),
    rank: 2
  },
  children: [
    {
      path: "/queueManage",
      name: "队列管理",
      component: () => import("@/views/queueManage/index.vue"),
      meta: {
        title: $t("menus.queueManage")
      }
    }
  ]
} satisfies RouteConfigsTable;
