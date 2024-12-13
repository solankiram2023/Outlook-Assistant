import asyncio
from agents.controller import process_input

if __name__ == "__main__":
    
    async def main():
        result = await process_input(
            user_input="Can you respond to this email?",
            user_email="kumar.sandee@northeastern.edu",
            email_context={
                "email_id": "AAMkADAwNDhkZDI3LThlODMtNDNkNy04ZGRjLWQwN2I1N2UxNjAyMABGAAAAAACDoa9wVVCtSYVLL53u_ueBBwBQwg2UQmqmTYVgJHd2DKqdAAAAAAEJAABQwg2UQmqmTYVgJHd2DKqdAAF612gfAAA=",
            }
        )
        print(result)

    # main()
    
    asyncio.run(main())