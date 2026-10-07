import streamlit as st
import time

st.set_page_config(
    page_title="Distributed RPC Model Serving Framework",
    page_icon="⚙️",
    layout="wide"
)

st.title("Distributed RPC Model Serving Framework")
st.caption("Visual dashboard for understanding Client → Orchestrator → Worker communication")

st.divider()

tab1, tab2, tab3 = st.tabs(
    ["System Architecture", "Run Request", "Project Files"]
)

with tab1:
    st.header("How the system works")

    col1, col2, col3 = st.columns(3)

    with col1:
        st.subheader("1. Client")
        st.write(
            """
            The Client sends a model inference request.

            Example:

            **Input:** `hello world`

            The client does not directly decide which worker should execute it.
            """
        )

    with col2:
        st.subheader("2. Orchestrator")
        st.write(
            """
            The Orchestrator acts like the manager.

            It:
            - keeps track of workers
            - receives requests
            - chooses a worker
            - routes requests
            - handles worker failures
            """
        )

    with col3:
        st.subheader("3. Worker")
        st.write(
            """
            The Worker performs the actual task.

            It:
            - registers with the orchestrator
            - receives requests
            - processes them
            - sends the result back
            """
        )

    st.divider()

    st.code(
        """
CLIENT
   |
   | RPC Request
   v
ORCHESTRATOR
   |
   | Select Worker
   v
WORKER
   |
   | Execute Request
   v
RESULT
   |
   v
CLIENT
""",
        language="text"
    )

with tab2:
    st.header("Visualize one request")

    request = st.text_input(
        "Enter a sample model request",
        value="Hello model"
    )

    if st.button("Send Request"):
        st.success("Request created")

        with st.status("Processing request...", expanded=True) as status:

            st.write("Client created RPC request...")
            time.sleep(0.7)

            st.write("Request sent to Orchestrator...")
            time.sleep(0.7)

            st.write("Orchestrator checking available workers...")
            time.sleep(0.7)

            st.write("Worker selected...")
            time.sleep(0.7)

            st.write("Worker executing request...")
            time.sleep(0.7)

            st.write("Worker returned response...")
            time.sleep(0.7)

            status.update(
                label="Request completed",
                state="complete"
            )

        st.subheader("Request")
        st.code(request)

        st.subheader("Sample response")
        st.code(f"Processed: {request}")

        st.warning(
            "Right now this tab visualizes the RPC flow. "
            "Next we will connect this button to your real C++ gRPC backend."
        )

with tab3:
    st.header("What each file in your repository does")

    st.subheader("protos/serving.proto")
    st.write(
        """
        This is the **contract** of your distributed system.

        It defines things such as:
        - RPC services
        - request messages
        - response messages
        - communication between components

        Both client and server understand each other because of this file.
        """
    )

    st.subheader("src/client.cpp")
    st.write(
        """
        This is the **request sender**.

        Its job is generally:

        1. connect to the RPC service
        2. create a request
        3. send the request
        4. wait for response
        5. display the result
        """
    )

    st.subheader("src/orchestrator.cpp")
    st.write(
        """
        This is the **manager of the distributed system**.

        Based on your resume, this component handles ideas such as:

        - worker registration
        - request routing
        - choosing workers
        - tracking available workers
        - handling failures
        """
    )

    st.subheader("src/worker.cpp")
    st.write(
        """
        This is the **execution node**.

        A worker receives work from the orchestrator,
        executes the task and returns the result.
        """
    )

    st.subheader("CMakeLists.txt")
    st.write(
        """
        This tells CMake how to compile the C++ project.

        It usually defines:
        - source files
        - executables
        - protobuf generation
        - gRPC libraries
        """
    )

    st.subheader("Dockerfile")
    st.write(
        """
        Defines how one project container is built.
        """
    )

    st.subheader("docker-compose.yml")
    st.write(
        """
        Lets multiple services run together.

        For a distributed project this can be useful for starting
        the orchestrator and multiple workers.
        """
    )

    st.subheader("README.md")
    st.write(
        """
        Documentation explaining how the project should be built,
        run and tested.
        """
    )