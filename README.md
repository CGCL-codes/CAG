# CAG
CAG is an efficient stream processing system that efficiently maintains load balance and efficiently provides a solution to consistent partitioning and minimizes aggregation cost. CAG limits the partition of data and processes the DAG in one node. We reduce the partition of data, so that stream processing's overall computation and aggregation cost remain low. The results of the experiment using large-scale real-world datasets show that CAG achieves a 3.5× improvement in terms of processing throughput and reduces the latency by 97% compared to state-of-the-art design. 

# Introduction
Current distributed stream processing systems like Storm, Flink, Samze, S4, etc. primary objective is to process data with high throughput at low latency. The architecture of these systems has a scale-out architecture to process an immense volume of data with a continuous data stream. Shuffle grouping , Key grouping, and partial key grouping are the important stream partitioning schemes use in these systems. Shuffle grouping sends tuples to  in a roundrobin style. The shuffle grouping is mostly used for stateless operators because when it is used for stateful operators, it an cause scalability issues in terms of memory. Additionally, communication cost increases when data is partitioned uniformly across operator instances. Shuffle  efficiently manages load balance. However, due to the scalability and heavy aggregation cost, it is not preferred for stateful operators. 



![alt text](https://github.com/mudassar66/CAG/blob/main/images/aggregation_cost.pdf?raw=true)
![alt text](https://github.com/mudassar66/CAG/tree/main/images/aggregation_cost.pdf?raw=true)


Use the package manager [pip](https://pip.pypa.io/en/stable/) to install foobar.

```bash
pip install foobar
```

## Usage


```python
import foobar

foobar.pluralize('word') # returns 'words'
foobar.pluralize('goose') # returns 'geese'
foobar.singularize('phenomena') # returns 'phenomenon'
```

## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

Please make sure to update tests as appropriate.

## License
[MIT](https://choosealicense.com/licenses/mit/)
