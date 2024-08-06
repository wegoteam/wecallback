import { $t } from "@/plugins/i18n";

export default {
  path: "/app",
  redirect: "/appManage",
  meta: {
    icon: "ant-design:database-outlined",
    // showLink: false,
    title: $t("menus.appManage"),
    rank: 4
  },
  children: [
    {
      path: "/appManage",
      name: "应用管理",
      component: () => import("@/views/appManage/index.vue"),
      meta: {
        title: $t("menus.appManage")
      }
    }
  ]
} satisfies RouteConfigsTable;
