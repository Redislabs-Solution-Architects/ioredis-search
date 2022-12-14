/**
 * @fileoverview Demo of Search features against Hashes with the ioredis client lib
 * @author Joey Whelan
 */

import Redis from 'ioredis';
import * as dotenv from 'dotenv';
const NUM = 10;
const COLORS = ['red', 'orange', 'yellow', 'green', 'blue', 'indigo', 'violet'];

async function load(client) {
    //Create pipeline to build 10 hash sets
    const pipeline = client.pipeline();
    for (let i=0; i < NUM; i++) {
        const colors = (COLORS.sort(() => .5 - Math.random())).slice(0, Math.floor(Math.random() * COLORS.length))
        const fields = {
            'textField': `text${Math.floor(Math.random() * NUM)}`, 
            'numericField': Math.floor(Math.random() * NUM), 
            'tagField': colors
        };
        await pipeline.hmset(`item:${i}`, fields);
    }
    await pipeline.exec();

    //Build index for those hashes
    await client.call('FT.CREATE', 'idx', 'ON', 'HASH', 'PREFIX', '1', 'item:', 'SCHEMA', 
        'textField', 'TEXT', 'SORTABLE',
        'numericField', 'NUMERIC', 'SORTABLE',
        'tagField', 'TAG'
    );

    //Print the INFO for the index
    const result = await client.call('FT.INFO', 'idx');
    console.log(`FT.INFO idx - ${JSON.stringify(result)}\n`);
}

async function search(client) {
    //Search for exact match in a Text field
    let result = await client.call('FT.SEARCH', 'idx', '@textField:text1');
    console.log(`FT.SEARCH idx @textField:text1 - ${JSON.stringify(result)}\n`);

    //Search for a range in a Numeric field
    result = await client.call('FT.SEARCH', 'idx', '@numericField:[1,3]');
    console.log(`FT.SEARCH idx @numericField:[1,3] - ${JSON.stringify(result)}\n`);

    //Search for a match in a Tag Field
    result = await client.call('FT.SEARCH', 'idx', '@tagField:{blue}');
    console.log(`FT.SEARCH idx @tagField:{blue} - ${JSON.stringify(result)}\n`);
}

async function aggregate(client) {
    //Aggregate on a text field across all hashes in the index
    let result = await client.call('FT.AGGREGATE', 'idx', '*', 'GROUPBY', '1', '@textField', 
        'REDUCE', 'COUNT', '0', 'AS', 'CNT');
    console.log(`FT.AGGREGATE idx * GROUPBY 1 @textField REDUCE COUNT 0 AS CNT - ${JSON.stringify(result)}\n`);

    //Search on a numeric range and then apply the SQRT function to a numeric field in the matches
    const upper = Math.floor(Math.random() * NUM) + 1;
    result = await client.call('FT.AGGREGATE', 'idx', `@numericField:[0,${upper}]`, 
        'APPLY', 'SQRT(@numericField)', 'AS', 'SQRT');
    console.log(`FT.AGGREGATE idx @numericField:[0,${upper}] APPLY SQRT(@numericField) AS SQRT - ${JSON.stringify(result)}\n`);

    //Search for logical OR of two tag values and use a CURSOR.  Limit return
    //to 2 values to illustrate looping on a cursor.
    result = await client.call('FT.AGGREGATE', 'idx', '@tagField:{ yellow | red }', 
        'LOAD', '3', '@textField', '@numericField', '@tagField',
        'WITHCURSOR', 'COUNT', '2'
    );
    console.log('FT.AGGREGATE idx @tagField:{ yellow | red } LOAD 3 @textField @numericField @tagField WITHCURSOR COUNT 2');
    let items = result[0];
    let cursor = result[1];
    while (true) {
        for (let item of items) {
            if (Array.isArray(item)) {
                console.log(JSON.stringify(item))
            }
        }
        if (cursor) {
            result = await client.call('FT.CURSOR', 'READ', 'idx', cursor, 'COUNT', '2');
            items = result[0];
            cursor = result[1];
        }
        else {
            break;
        }
    }
    console.log();
}

async function alter(client) {
    console.log('Adding additional text field "newField" to all hashes and altering index');
    
    //Set up a pipeline to add a new field to all the existing hashes.
    const pipeline = client.pipeline();
    for (let i=0; i < NUM; i++) {
        await pipeline.hset(`item:${i}`, 'newField', `new${Math.floor(Math.random() * NUM)}`)
    }
    await pipeline.exec();

    //Alter the existing index by adding this new field
    await client.call('FT.ALTER', 'idx', 'SCHEMA', 'ADD', 'newField', 'TEXT', 'SORTABLE');
    let result = await client.call('FT.INFO', 'idx');
    console.log(`FT.INFO idx - ${JSON.stringify(result)}\n`);

    //Search for this new field in the index
    result = await client.call('FT.SEARCH', 'idx', '@newField:new1');
    console.log(`FT.SEARCH idx @newField:new1 - ${JSON.stringify(result)}\n`);
}

(async () => {
    try {
        dotenv.config();
        var client = new Redis(process.env.REDIS_URL);
        await load(client);
        await search(client);
        await aggregate(client);
        await alter(client);
    }
    catch (err) {
        console.error(err);
    }
    finally {
        await client.quit();
    }
})();