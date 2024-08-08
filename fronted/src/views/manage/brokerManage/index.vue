<template>
  <PageWrapper dense contentFullHeight fixedHeight contentClass="flex">
    <BasicTable @register="registerTable" :searchInfo="searchInfo">
      <template #toolbar>
        <a-button type="primary" @click="handleCreate">新增应用</a-button>
      </template>
      <template #bodyCell="{ column, record }">
        <template v-if="column.key === 'action'">
          <TableAction
            :actions="[
              {
                icon: 'clarity:info-standard-line',
                tooltip: '查看应用详情',
                onClick: handleView.bind(null, record),
              },
              {
                icon: 'clarity:note-edit-line',
                tooltip: '编辑应用',
                onClick: handleEdit.bind(null, record),
              },
              {
                icon: 'ant-design:delete-outlined',
                color: 'error',
                tooltip: '删除此应用',
                popConfirm: {
                  title: '是否确认删除',
                  placement: 'left',
                  confirm: handleDelete.bind(null, record),
                },
              },
            ]"
          />
        </template>
      </template>
    </BasicTable>
  </PageWrapper>

</template>
<script lang="ts">
import { defineComponent, reactive } from 'vue';

import {BasicTable, useTable, TableAction} from '/@/components/Table';
import { getAppList } from '/@/api/appManage/app';
import { PageWrapper } from '/@/components/Page';

import { useModal } from '/@/components/Modal';

import { useGo } from '/@/hooks/web/usePage';

export default defineComponent({
  name: 'BrokerManage',
  components: { BasicTable, PageWrapper, TableAction },
  setup() {
    const go = useGo();
    const [registerModal, { openModal }] = useModal();
    const searchInfo = reactive<Recordable>({});

    const [registerTable, { reload, updateTableDataRecord }] = useTable({
      title: '应用列表',
      api: getAppList,
      rowKey: 'appid',
      columns: [
        {
          title: '应用编号',
          dataIndex: 'appcode',
          width: 120,
        },
        {
          title: '应用名称',
          dataIndex: 'appname',
          width: 120,
        },
        {
          title: '状态',
          dataIndex: 'appstate',
          width: 120,
          customRender: ({ record }) => {
            return record.appstate === 0 ? '启用' : '停用';
          }
        },
        {
          title: '创建时间',
          dataIndex: 'createTime',
          width: 180,
        },
        {
          title: '修改时间',
          dataIndex: 'editTime',
          width: 200,
        },
        {
          title: '备注',
          dataIndex: 'remark',
        },
      ],
      formConfig: {
        labelWidth: 120,
        schemas: [
          {
            field: 'appcode',
            label: '应用编号',
            component: 'Input',
            colProps: { span: 8 },
          },
          {
            field: 'appname',
            label: '应用名称',
            component: 'Input',
            colProps: { span: 8 },
          },
        ],
        autoSubmitOnEnter: true,
      },
      useSearchForm: true,
      showTableSetting: true,
      bordered: true,
      handleSearchInfoFn(info) {
        console.log('handleSearchInfoFn', info);
        return info;
      },
      actionColumn: {
        width: 120,
        title: '操作',
        dataIndex: 'action',
        // slots: { customRender: 'action' },
      },
    });

    function handleCreate() {
      openModal(true, {
        isUpdate: false,
      });
    }

    function handleEdit(record: Recordable) {
      // console.log(record);
      openModal(true, {
        record,
        isUpdate: true,
      });
    }

    function handleDelete(record: Recordable) {
      console.log(record);
    }

    function handleSuccess({ isUpdate, values }) {
      if (isUpdate) {
        // 演示不刷新表格直接更新内部数据。
        // 注意：updateTableDataRecord要求表格的rowKey属性为string并且存在于每一行的record的keys中
        const result = updateTableDataRecord(values.id, values);
        console.log(result);
      } else {
        reload();
      }
    }

    function handleSelect(deptId = '') {
      searchInfo.deptId = deptId;
      reload();
    }

    function handleView(record: Recordable) {
      console.log(record)
      go('/system/account_detail/' + record.id);
    }

    return {
      registerTable,
      registerModal,
      handleCreate,
      handleEdit,
      handleDelete,
      handleSuccess,
      handleSelect,
      handleView,
      searchInfo,
    };
  },
});
</script>

<style scoped>

</style>

