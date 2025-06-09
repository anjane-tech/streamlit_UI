import preswald as st

page = st.selectbox("Choose a Page", ["Hello", "Callitrichidae Analysis V2", "Employee & Department"])

if page == "Hello":
    namespace = {}
    with open("hello.py") as f:
        exec(f.read(), namespace)
    namespace["render"]()

elif page == "Callitrichidae Analysis V2":
    namespace = {}
    with open("analyze_employee_data_v2.py") as f:
        exec(f.read(), namespace)

elif page == "Employee & Department":
    namespace = {}
    with open("diff.py") as f:
        exec(f.read(), namespace)
