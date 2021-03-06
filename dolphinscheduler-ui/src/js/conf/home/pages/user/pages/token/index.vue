/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
<template>
  <m-list-construction :title="$t('Token manage')">
    <template slot="conditions">
      <m-conditions @on-conditions="_onConditions">
        <template slot="button-group">
          <x-button type="ghost" size="small" @click="_create('')">{{$t('Create token')}}</x-button>
        </template>
      </m-conditions>
    </template>
    <template slot="content">
      <template v-if="tokenList.length">
        <m-list @on-edit="_onEdit"
                :token-list="tokenList"
                :page-no="searchParams.pageNo"
                :page-size="searchParams.pageSize">

        </m-list>
        <div class="page-box">
          <x-page :current="parseInt(searchParams.pageNo)" :total="total" :page-size="searchParams.pageSize" show-elevator @on-change="_page" show-sizer :page-size-options="[10,30,50]" @on-size-change="_pageSize"></x-page>
        </div>
      </template>
      <template v-if="!tokenList.length">
        <m-no-data></m-no-data>
      </template>
      <m-spin :is-spin="isLoading"></m-spin>
    </template>
  </m-list-construction>
</template>
<script>
  import _ from 'lodash'
  import { mapActions } from 'vuex'
  import mList from './_source/list'
  import mSpin from '@/module/components/spin/spin'
  import mCreateToken from './_source/createToken'
  import mNoData from '@/module/components/noData/noData'
  import listUrlParamHandle from '@/module/mixin/listUrlParamHandle'
  import mConditions from '@/module/components/conditions/conditions'
  import mSecondaryMenu from '@/module/components/secondaryMenu/secondaryMenu'
  import mListConstruction from '@/module/components/listConstruction/listConstruction'

  export default {
    name: 'token-index',
    data () {
      return {
        total: null,
        isLoading: false,
        tokenList: [],
        searchParams: {
          pageSize: 10,
          pageNo: 1,
          searchVal: ''
        }
      }
    },
    mixins: [listUrlParamHandle],
    props: {},
    methods: {
      ...mapActions('user', ['getTokenListP']),
      /**
       * Inquire
       */
      _onConditions (o) {
        this.searchParams = _.assign(this.searchParams, o)
        this.searchParams.pageNo = 1
      },
      _page (val) {
        this.searchParams.pageNo = val
      },
      _pageSize (val) {
        this.searchParams.pageSize = val
      },
      _onEdit (item) {
        this._create(item)
      },
      _create (item) {
        let self = this
        let modal = this.$modal.dialog({
          closable: false,
          showMask: true,
          escClose: true,
          className: 'v-modal-custom',
          transitionName: 'opacityp',
          render (h) {
            return h(mCreateToken, {
              on: {
                onUpdate () {
                  self._debounceGET('false')
                  modal.remove()
                },
                close () {
                  modal.remove()
                }
              },
              props: {
                item: item
              }
            })
          }
        })
      },
      _getList (flag) {
        this.isLoading = !flag
        this.getTokenListP(this.searchParams).then(res => {
          this.tokenList = []
          this.tokenList = res.totalList
          this.total = res.total
          this.isLoading = false
        }).catch(e => {
          this.isLoading = false
        })
      }
    },
    watch: {
      // router
      '$route' (a) {
        // url no params get instance list
        this.searchParams.pageNo = _.isEmpty(a.query) ? 1 : a.query.pageNo
      }
    },
    created () {
    },
    mounted () {
      this.$modal.destroy()
    },
    components: { mSecondaryMenu, mList, mListConstruction, mConditions, mSpin, mNoData }
  }
</script>
