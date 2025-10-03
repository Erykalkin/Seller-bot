async def group_parser():
    while not stop_group_parser.is_set():
        users = db.get_target_users_with_info()
        new_users = [(uid, acs_map, info) for (uid, acs_map, info) in users if not db.has_user(uid)]
        print(f"Найдено {len(new_users)} новых пользователей")

        for user_id, access_map, info in new_users:
            executor, access_hash = db.choose_executor_from_map(access_map)
            bot = pool.get_client(executor)
            await db.add_user(bot, user_id, executor=executor, access_hash=access_hash, info=info)

        await asyncio.sleep(UPDATE_BD_PERIOD)