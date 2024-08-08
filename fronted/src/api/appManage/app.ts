import {
  AppPageResult,
  AppPageParams,
} from './appModel';
import { defHttp } from '/@/utils/http/axios';

enum Api {
  AppManageList = '/manage/app/getAppList',
}

export const getAppList = (params: AppPageParams) =>
  defHttp.post<AppPageResult>({ url: Api.AppManageList, params });
