import { $t } from "@/plugins/i18n";

export default {
  path: "/comsumerManage",
  redirect: "/comsumerManage",
  meta: {
    icon: "majesticons:atom-2-line",
    // showLink: false,
    title: $t("menus.comsumerManage"),
    rank: 3
  },
  children: [
    {
      path: "/comsumerManage",
      name: "消费者管理",
      component: () => import("@/views/comsumerManage/index.vue"),
      meta: {
        title: $t("menus.comsumerManage")
      }
    }
  ]
} satisfies RouteConfigsTable;
