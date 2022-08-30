/**
 * @fileoverview Demo of Search features against Hashes with the ioredis client lib
 * @author Joey Whelan
 */

import Redis from 'ioredis';
import * as dotenv from 'dotenv';
const NUM = 10;
const COLORS = ['red', 'orange', 'yellow', 'green', 'blue', 'indigo', 'violet'];

async function load(client) {
    let i;
    for (i=0; i < NUM; i++) {
        const colors = (COLORS.sort(() => .5 - Math.random())).slice(0, Math.floor(Math.random() * COLORS.length))
        const fields = {
            'textField': `text${Math.floor(Math.random() * NUM)}`, 
            'numericField': Math.floor(Math.random() * NUM), 
            'tagField': colors
        };
        await client.hmset(`item:${i}`, fields);
    }
    await client.call('FT.CREATE', 'idx', 'ON', 'HASH', 'PREFIX', '1', 'item:', 'SCHEMA', 
        'textField', 'TEXT', 'SORTABLE',
        'numericField', 'NUMERIC', 'SORTABLE',
        'tagField', 'TAG'
    );
    
    const result = await client.call('FT.INFO', 'idx');
    console.log(`FT.INFO idx - ${JSON.stringify(result)}\n`);
}

async function search(client) {
    let result = await client.call('FT.SEARCH', 'idx', '@textField:text1');
    console.log(`FT.SEARCH idx @textField:text1 - ${JSON.stringify(result)}\n`);

    result = await client.call('FT.SEARCH', 'idx', '@numericField:[1,3]');
    console.log(`FT.SEARCH idx @numericField:[1,3] - ${JSON.stringify(result)}\n`);

    result = await client.call('FT.SEARCH', 'idx', '@tagField:{blue}');
    console.log(`FT.SEARCH idx @tagField:{blue} - ${JSON.stringify(result)}\n`);
}

async function aggregate(client) {
    let result = await client.call('FT.AGGREGATE', 'idx', '*', 'GROUPBY', '1', '@textField', 
        'REDUCE', 'COUNT', '0', 'AS', 'CNT');
    console.log(`FT.AGGREGATE idx * GROUPBY 1 @textField REDUCE COUNT 0 AS CNT - ${JSON.stringify(result)}\n`);

    const upper = Math.floor(Math.random() * NUM) + 1;
    result = await client.call('FT.AGGREGATE', 'idx', `@numericField:[0,${upper}]`, 
        'APPLY', 'SQRT(@numericField)', 'AS', 'SQRT');
    console.log(`FT.AGGREGATE idx @numericField:[0,${upper}] APPLY SQRT(@numericField) AS SQRT - ${JSON.stringify(result)}\n`);

    result = await client.call('FT.AGGREGATE', 'idx', '@tagField:{yellow | red}', 
        'LOAD', '3', '@textField', '@numericField', '@tagField',
        'WITHCURSOR', 'COUNT', '2'
    );
    console.log('FT.AGGREGATE idx @tagField:{yellow | red} LOAD 3 @textField @numericField @tagField WITHCURSOR COUNT 2'
    );
    let items = result[0];
    let cursor = result[1];
    do {
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
    } while (cursor);
    console.log();
}

async function alter(client) {
    console.log('Adding additional text field "newField" to all hashes and altering index');
    for (let i=0; i < NUM; i++) {
        await client.hset(`item:${i}`, 'newField', `new${Math.floor(Math.random() * NUM)}`)
    }
    await client.call('FT.ALTER', 'idx', 'SCHEMA', 'ADD', 'newField', 'TEXT', 'SORTABLE');
    let result = await client.call('FT.INFO', 'idx');
    console.log(`FT.INFO idx - ${JSON.stringify(result)}\n`);

    result = await client.call('FT.SEARCH', 'idx', '@newField:new1');
    console.log(`FT.SEARCH idx @newField:new1 - ${JSON.stringify(result)}\n`);
}

(async () => {
    try {
        dotenv.config();
        var client = new Redis(process.env.REDIS_URL || '');
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

