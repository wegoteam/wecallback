import { BasicPageParams, BasicFetchResult } from '/@/api/model/baseModel';

export type AppPageParams = BasicPageParams & {
  appcode?: string;
  appname?: string;
};

export interface AppPageResult {
  appid: string;
  appcode: string;
  appname: string;
  appstate: number;
  createTime: string;
  editTime: string;
  remark: string;
}


export type AppPageResultModel = BasicFetchResult<AppPageResult>;


