import { split } from './shlex'
import { SQLParams, SQLDataType } from './sql'

export type ISchema = Record<string, {
  type?: 'string' | 'number' | 'date' | 'boolean'
  isAny?: boolean
}>

export class EasyQuery {
  constructor (
    public options: {
      dialect?: 'sqlite',
      schema?: ISchema
      normalizeDates?: ((d: any) => string | number | null)
    }
  ) {}

  get dialect () {
    return this.options.dialect || 'sqlite'
  }

  get schema () {
    return this.options.schema || {} as ISchema
  }

  normalizeDates (d: any): string | number | null {
    const fn = this.options.normalizeDates || ((d: any) => +new Date(d))
    return fn(d)
  }

  parse (q: any): {
    params: SQLParams,
    statement: string,
    bindings: Record<string, SQLDataType>
  } {
    if (typeof q === 'string') {
      return this.parseQ(q)
    }
    return this.parseCond(q)
  }

  parseQ (q: string): {
    params: SQLParams,
    statement: string,
    bindings: Record<string, SQLDataType>
  } {
    const params = new SQLParams()
    let $or = [] as string[]
    const $and = [] as string[]

    split(q).map((el) => {
      // eslint-disable-next-line no-useless-escape
      const [op] = /^[\-+?]/.exec(el) || [] as string[]
      if (op) {
        el = el.substr(1)
      }

      const addOp = (k: string, opK: string, v: any) => {
        let isDate = false

        if (v && this.schema[k]) {
          if (!this.schema[k].type || this.schema[k].type === 'string') {
            ;
          } else if (this.schema[k].type === 'number') {
            ;
          } else if (this.schema[k].type === 'boolean') {
            ;
          } else if (this.schema[k].type === 'date') {
            if (v === 'NOW') {
              v = new Date()
            } else {
              const vMillisec = (() => {
                const [_, p1, p2] = /^([+-]?\d+(?:\.\d+)?)([yMwdhm])$/i.exec(v) || []
                const v0 = +new Date()
                if (p2 === 'y') {
                  return v0 + parseFloat(p1) * 365 * 24 * 60 * 60 * 1000 // 365d 24h 60m 60s 1000ms
                } else if (p2 === 'M') {
                  return v0 + parseFloat(p1) * 30 * 24 * 60 * 60 * 1000 // 30d 24h 60m 60s 1000ms
                } else if (p2 === 'w') {
                  return v0 + parseFloat(p1) * 7 * 24 * 60 * 60 * 1000 // 7d 24h 60m 60s 1000ms
                } else if (p2 === 'd') {
                  return v0 + parseFloat(p1) * 24 * 60 * 60 * 1000 // 24h 60m 60s 1000ms
                } else if (p2 === 'h') {
                  return v0 + parseFloat(p1) * 60 * 60 * 1000 // 60m 60s 1000ms
                } else if (p2 === 'm') {
                  return v0 + parseFloat(p1) * 60 * 1000 // 60s 1000ms
                }
                return null
              })()

              v = vMillisec ? new Date(vMillisec) : v
            }

            v = this.normalizeDates(v)
            isDate = true
          }
        }

        if (op === '+') {
          return `${k} = ${params.add(v)}`
        } else if (op === '-') {
          if (typeof v === 'string' && !isDate) {
            return `${k} NOT LIKE '%'||${params.add(v)}||'%'`
          } else if (opK === '>' && (typeof v === 'number' || isDate)) {
            return `${k} <= ${params.add(v)}`
          } else if (opK === '<' && (typeof v === 'number' || isDate)) {
            return `${k} >= ${params.add(v)}`
          } else {
            return `${k} != ${params.add(v)}`
          }
        } else {
          if (typeof v === 'string' && !isDate) {
            return `${k} LIKE '%'||${params.add(v)}||'%'`
          } else if (opK === '>' && (typeof v === 'number' || isDate)) {
            return `${k} > ${params.add(v)}`
          } else if (opK === '<' && (typeof v === 'number' || isDate)) {
            return `${k} < ${params.add(v)}`
          }

          return `${k} = ${params.add(v)}`
        }
      }

      const [k, opK, v] = el.split(/([:><])(.+)/)

      if (v === 'NULL') {
        if (op === '-') {
          $and.push(
            `${k} IS NOT NULL`
          )
          return
        } else if (op === '?') {
          $or.push(
            `${k} IS NULL`
          )
        } else {
          $and.push(
            `${k} IS NULL`
          )
        }
        return
      }

      let subCond = ''

      if (v) {
        subCond = addOp(k, opK, v)
      } else if (this.schema) {
        subCond = Object.entries(this.schema)
          .filter(([_, v0]) => (!v0.type || v0.type === 'string') && v0.isAny !== false)
          .map(([k0, _]) => addOp(k0, opK, k))
          .join(op === '-' ? ' AND ' : ' OR ')
      }

      if (subCond) {
        if (op === '?') {
          $or.push(subCond)
        } else {
          $and.push(subCond)
        }
      }
    })

    $or.push($and.join(' AND '))
    $or = $or.filter(el => el)

    const output = $or.join(' OR ')

    return {
      statement: output || 'TRUE',
      params: params,
      bindings: params.data
    }
  }

  parseCond (
    cond: Record<string, any>
  ): {
    params: SQLParams,
    statement: string,
    bindings: Record<string, SQLDataType>
  } {
    const parseCond = (q: any) => {
      const subClause: string[] = []

      if (Array.isArray(q.$or)) {
        subClause.push(q.$or.map((el: any) => parseCond(el)).join(' OR '))
      } else if (Array.isArray(q.$and)) {
        subClause.push(q.$and.map((el: any) => parseCond(el)).join(' AND '))
      } else {
        subClause.push(parseCondBasic(q))
      }

      if (subClause.length > 0) {
        return subClause.join(' AND ')
      }

      return 'TRUE'
    }

    const parseCondBasic = (cond: any) => {
      const cList: string[] = []

      function doDefault (k: string, v: any) {
        cList.push(`${k} = ${params.add(v)}`)
      }

      for (let [k, v] of Object.entries(cond)) {
        let isPushed = false
        if (k.includes('.')) {
          const kn = k.split('.')
          k = `json_extract(${kn[0]}, '$.${kn.slice(1).join('.')}')`
        }

        if (v instanceof Date) {
          v = this.normalizeDates(v)
        }

        if (v) {
          if (Array.isArray(v)) {
            if (v.length > 1) {
              cList.push(`${k} IN (${v.map((v0) => `${params.add(v0)}`).join(',')})`)
            } else if (v.length === 1) {
              cList.push(`${k} = ${params.add(v[0])}`)
            }
          } else if (isObject(v)) {
            const op = Object.keys(v)[0]
            let v1 = v[op]
            if (v1 instanceof Date) {
              v1 = +v1
            }

            if (Array.isArray(v1)) {
              switch (op) {
                case '$in':
                  if (v1.length > 1) {
                    cList.push(`${k} IN (${v1.map((v0) => params.add(v0)).join(',')})`)
                  } else if (v1.length === 1) {
                    cList.push(`${k} = ${params.add(v1[0])}`)
                  }
                  isPushed = true
                  break
                case '$nin':
                  if (v1.length > 1) {
                    cList.push(`${k} NOT IN (${v1.map((v0) => params.add(v0)).join(',')})`)
                  } else {
                    cList.push(`${k} != ${params.add(v1[0])}`)
                  }
                  isPushed = true
                  break
              }
            }

            if (isPushed) {
              continue
            }

            if (v1 && typeof v1 === 'object') {
              if (v1 instanceof Date) {
                k = `json_extract(${k}, '$.$milli')`
                v1 = +v1
              } else {
                v1 = JSON.stringify(v1)
              }
            }

            switch (op) {
              case '$like':
                cList.push(`${k} LIKE ${params.add(v1)}`)
                break
              case '$nlike':
                cList.push(`${k} NOT LIKE ${params.add(v1)}`)
                break
              case '$substr':
                cList.push(`${k} LIKE '%'||${params.add(v1)}||'%'`)
                break
              case '$nsubstr':
                cList.push(`${k} NOT LIKE '%'||${params.add(v1)}||'%'`)
                break
              case '$exists':
                cList.push(`${k} IS ${v1 ? 'NOT NULL' : 'NULL'}`)
                break
              case '$gt':
                cList.push(`${k} > ${params.add(v1)}`)
                break
              case '$gte':
                cList.push(`${k} >= ${params.add(v1)}`)
                break
              case '$lt':
                cList.push(`${k} < ${params.add(v1)}`)
                break
              case '$lte':
                cList.push(`${k} <= ${params.add(v1)}`)
                break
              case '$ne':
                cList.push(`${k} != ${params.add(v1)}`)
                break
              default:
                doDefault(k, v)
            }
          } else {
            doDefault(k, v)
          }
        } else {
          doDefault(k, v)
        }
      }

      if (cList.length > 0) {
        return cList.join(' AND ')
      }
      return 'TRUE'
    }

    const params = new SQLParams()
    const statement = parseCond(cond)

    return {
      statement,
      params,
      bindings: params.data
    }
  }

  parseSelect (projection: Record<string, number>) {
    return Object.entries(projection).map(([k, v]) => {
      if (k !== 'id') {
        return v === 1 ? k : undefined
      } else {
        return v === 0 ? undefined : k
      }
    }).filter((el) => el).map((el) => `"${(el as string).replace(/"/g, '[$&]')}"`).join(',')
  }
}

export * from './sql'

function isObject (o: any): o is Record<string, any> {
  return !!o && typeof o === 'object' && o.constructor === Object
}
