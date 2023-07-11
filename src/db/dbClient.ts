import { MongoClient, ObjectId } from 'mongodb'
import dotenv from 'dotenv'
import { InvalidRequestError } from '@atproto/xrpc-server'

dotenv.config()

class dbSingleton {
  client: MongoClient | null = null

  constructor(connection_string: string) {
    this.client = new MongoClient(connection_string)
    this.init()
  }

  async init() {
    if (this.client === null) throw new Error('DB Cannot be null')
    await this.client.connect()
  }

  async deleteManyURI(collection: string, uris: string[]) {
    await this.client
      ?.db()
      .collection(collection)
      .deleteMany({ uri: { $in: uris } })
  }

  async deleteManyDID(collection: string, dids: string[]) {
    await this.client
      ?.db()
      .collection(collection)
      .deleteMany({ did: { $in: dids } })
  }

  async replaceOneURI(collection: string, uri: string, data: any) {
    if (!(typeof data._id === typeof '')) data._id = new ObjectId()
    else {
      data._id = new ObjectId(data._id)
    }

    await this.client
      ?.db()
      .collection(collection)
      .replaceOne({ uri: uri }, data, { upsert: true })
  }

  async replaceOneDID(collection: string, did: string, data: any) {
    if (!(typeof data._id === typeof '')) data._id = new ObjectId()
    else {
      data._id = new ObjectId(data._id)
    }

    await this.client
      ?.db()
      .collection(collection)
      .replaceOne({ did: did }, data, { upsert: true })
  }

  async aggregatePostsByReplies(
    collection: string,
    tag: string,
    threshold: number,
    limit = 50,
    cursor: string | undefined = undefined,
  ) {
    let start = 0

    if (cursor !== undefined) {
      start = Number.parseInt(cursor)
    }

    const posts = await this.client
      ?.db()
      .collection(collection)
      .aggregate([
        { $match: { algoTags: tag, replyRoot: { $ne: null } } },
        { $group: { _id: '$replyRoot', count: { $sum: 1 } } },
        { $match: { count: { $gt: threshold } } },
        { $sort: { count: -1 } },
      ])
      .toArray()

    if (posts?.length !== undefined && posts.length > 0) {
      const returned_posts = posts.sort((a, b) => {
        return Number.parseFloat(b.count) - Number.parseFloat(a.count)
      })
      return returned_posts.slice(start, limit + start)
    } else return []
  }

  async updateSubStateCursor(service: string, cursor: number) {
    await this.client
      ?.db()
      .collection('sub_state')
      .findOneAndReplace(
        { service: service },
        { service: service, cursor: cursor },
      )
  }

  async getSubStateCursor(service: string) {
    return await this.client
      ?.db()
      .collection('sub_state')
      .findOne({ service: service })
  }

  async getLatestPostsForTag(
    tag: string,
    limit = 50,
    cursor: string | undefined = undefined,
  ) {
    let query: { indexedAt?: any; cid?: any; algoTags: string } = {
      algoTags: tag,
    }

    if (cursor !== undefined) {
      const [indexedAt, cid] = cursor.split('::')
      if (!indexedAt || !cid) {
        throw new InvalidRequestError('malformed cursor')
      }
      const timeStr = new Date(parseInt(indexedAt, 10)).getTime()

      query = { indexedAt: { $lte: timeStr }, cid: { $ne: cid }, algoTags: tag }
    }

    const results = this.client
      ?.db()
      .collection('post')
      .find(query)
      .sort({ indexedAt: -1, cid: -1 })
      .limit(limit)
      .toArray()

    if (results === undefined) return []
    else return results
  }

  async getTaggedPostsBetween(tag: string, start: number, end: number) {
    const larger = start > end ? start : end
    const smaller = start > end ? end : start

    const results = this.client
      ?.db()
      .collection('post')
      .find({ indexedAt: { $lt: larger, $gt: smaller }, algoTags: tag })
      .sort({ indexedAt: -1, cid: -1 })
      .toArray()

    if (results === undefined) return []
    else return results
  }

  async getRecentAuthorsForTag(tag: string, lastMs: number = 600000) {
    const results = await this.client
      ?.db()
      .collection('post')
      .distinct('author', {
        indexedAt: { $gt: new Date().getTime() - lastMs },
        algoTags: tag,
      })

    if (results === undefined) return []
    else return results
  }

  async getDistinctFromCollection(collection: string, field: string) {
    const results = await this.client
      ?.db()
      .collection(collection)
      .distinct(field)
    if (results === undefined) return []
    else return results
  }

  async removeTagFromPostsForAuthor(tag: string, authors: string[]) {
    const pullQuery: Record<string, any> = { algoTags: { $in: [tag] } }
    await this.client
      ?.db()
      .collection('post')
      .updateMany({ author: { $in: authors } }, { $pull: pullQuery })

    await this.deleteUntaggedPosts()
  }

  async removeTagFromOldPosts(tag: string, indexedAt: number) {
    const pullQuery: Record<string, any> = { algoTags: { $in: [tag] } }
    await this.client
      ?.db()
      .collection('post')
      .updateMany({ indexedAt: { $lt: indexedAt } }, { $pull: pullQuery })

    await this.deleteUntaggedPosts()
  }

  async deleteUntaggedPosts() {
    await this.client
      ?.db()
      .collection('post')
      .deleteMany({ algoTags: { $size: 0 } })
  }

  async getPostForURI(uri: string) {
    const results = await this.client
      ?.db()
      .collection('post')
      .findOne({ uri: uri })
    if (results === undefined) return null
    return results
  }
}

const dbClient = new dbSingleton(
  `${process.env.FEEDGEN_MONGODB_CONNECTION_STRING}`,
)

export default dbClient
