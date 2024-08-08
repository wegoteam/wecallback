import {
  AppPageParams,
  AppPageResultModel,
} from './appModel';
import { defHttp } from '/@/utils/http/axios';

enum Api {
  AppManageList = '/manage/app/getAppList',
}

export const getAppList = (params: AppPageParams) =>
  defHttp.post<AppPageResultModel>({ url: Api.AppManageList, params });
