import { MockMethod } from 'vite-plugin-mock';
import { resultError, resultPageSuccess, resultSuccess } from '../_util';

const accountList = (() => {
  const result: any[] = [];
  for (let index = 0; index < 20; index++) {
    result.push({
      appid: `${index}`,
      appcode: '@first',
      appname: '@cname()',
      nickname: '@cname()',
      createTime: '@datetime',
      editTime: '@datetime',
      remark: '@cword(10,20)',
      'appstate|1': [0, 1],
    });
  }
  return result;
})();


export default [
  {
    url: '/basic-api/manage/app/getAppList',
    timeout: 100,
    method: 'post',
    response: ({ query }) => {
      const { page = 1, pageSize = 20 } = query;
      return resultPageSuccess(page, pageSize, accountList);
    },
  },
  {
    url: '/basic-api/manage/app/getAppList',
    timeout: 100,
    method: 'get',
    response: ({ query }) => {
      const { page = 1, pageSize = 20 } = query;
      return resultPageSuccess(page, pageSize, accountList);
    },
  },
] as MockMethod[];
