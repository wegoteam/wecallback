<template>
  <BasicModal v-bind="$attrs" @register="registerModal" :title="getTitle" @ok="handleSubmit">
    <BasicForm @register="registerForm" />
  </BasicModal>
</template>
<script lang="ts">
  import { defineComponent, ref, computed, unref } from 'vue';
  import { BasicModal, useModalInner } from '/@/components/Modal';
  import {BasicForm, useForm} from '/@/components/Form/index';
  import {AppStateEnum, AppStateNameEnum} from '/@/enums/appManageEnum';

  export default defineComponent({
    name: 'AppModal',
    components: { BasicModal, BasicForm },
    emits: ['success', 'register'],
    setup(_, { emit }) {
      const isUpdate = ref(true);
      const rowId = ref('');

      const [registerForm, { setFieldsValue, resetFields, validate }] = useForm({
        labelWidth: 100,
        baseColProps: { span: 24 },
        schemas: [
          {
            field: 'appcode',
            label: '应用编号',
            component: 'Input',
            helpMessage: ['输入的应用编号不能重复'],
            rules: [
              {
                required: true,
                message: '请输入应用编号',
              },
            ],
          },
          {
            field: 'appname',
            label: '应用名称',
            component: 'Input',
            required: true,
          },

          {
            label: '状态',
            field: 'appstate',
            component: 'Select',
            defaultValue: AppStateEnum.ENABLE,
            componentProps: {
              options: [
                {
                  label: AppStateNameEnum.ENABLE,
                  value: AppStateEnum.ENABLE,
                },
                {
                  label: AppStateNameEnum.DISABLE,
                  value: AppStateEnum.DISABLE,
                },
              ],
            },
            required: true,
          },

          {
            label: '备注',
            field: 'remark',
            component: 'InputTextArea',
          },
        ],
        showActionButtonGroup: false,
        actionColOptions: {
          span: 23,
        },
      });

      const [registerModal, { setModalProps, closeModal }] = useModalInner(async (data) => {
        resetFields();
        setModalProps({ confirmLoading: false });
        isUpdate.value = !!data?.isUpdate;

        if (unref(isUpdate)) {
          rowId.value = data.record.id;
          setFieldsValue({
            ...data.record,
          });
        }

      });

      const getTitle = computed(() => (!unref(isUpdate) ? '新增应用' : '编辑应用'));

      async function handleSubmit() {
        try {
          const values = await validate();
          setModalProps({ confirmLoading: true });
          // TODO custom api
          console.log(values);
          closeModal();
          emit('success', { isUpdate: unref(isUpdate), values: { ...values, id: rowId.value } });
        } finally {
          setModalProps({ confirmLoading: false });
        }
      }

      return { registerModal, registerForm, getTitle, handleSubmit };
    },
  });
</script>
