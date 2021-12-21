# Proof of Concept IT/OT convergence middleware
**Abstract**:
We are still in the midst of Industry 4.0 (I4.0), with more manufacturing lines being labeled as smart thanks to the integration of advanced ICT in Cyber-Physical Systems (CPS). While I4.0 aims to provision cognitive CPS systems, the nascent Industry 5.0 (I5.0) era goes a step beyond, aiming to build cross-border, sustainable, and circular value chains benefiting society as a whole.
An enabler of this vision is the integration of data and AI in the industrial decision-making process, which does not exhibit yet a coordination between the Operation and Information Technology domains (OT/IT). This work proposes an architectural approach and an accompanying software prototype addressing the OT/IT convergence problem. The approach is based on a two-layered middleware solution, where each layer aims to better serve the specific differentiated requirements of the OT and IT layers. The proposal is validated in a real testbed, employing actual machine data, so showing the capacity of the components to gracefully scale and serve increasing data volumes.

If you use this code please cite: 
```
@Article{mw-itot-convergence,
	author = {Patera, Lorenzo and Garbugli, Andrea and Bujari, Armir and Scotece, Domenico and Corradi, Antonio},
	title = {A Layered Middleware for OT/IT Convergence to Empower Industry 5.0 Applications},
	journal = {Sensors},
	year = {2021},
}
```

## Requirements
This software has been tested with Apache Kafka 6.2.0 and ZooKeeper 6.2.0. All required software can be run using Docker images:
- confluentinc/cp-kafka:6.2.0
- confluentinc/cp-zookeeper:6.2.0

## Build and run
This project uses json-c and RdKafka dependences that must be installed by using vcpkg tool.

```
vcpkg install json-c
vcpkg install RdKafka
```

Then the software can be build by using cmake
```
cmake --build .
```