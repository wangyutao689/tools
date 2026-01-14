package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/IBM/sarama"
)

// Topic 是导出/导入的 JSON 结构
type Topic struct {
	Name              string            `json:"name"`
	Partitions        int32             `json:"partitions"`
	ReplicationFactor int16             `json:"replication_factor"`
	Configs           map[string]string `json:"configs,omitempty"`
}

// ExportFile 是整个导出文件的结构
type ExportFile struct {
	KafkaVersion string  `json:"kafka_version"`
	ExportTime   string  `json:"export_time"`
	Topics       []Topic `json:"topics"`
}

// newAdmin 创建 Sarama ClusterAdmin
func newAdmin(broker string) (sarama.ClusterAdmin, error) {
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V2_4_0_0
	cfg.Admin.Timeout = 10 * time.Second
	return sarama.NewClusterAdmin([]string{broker}, cfg)
}

// exportTopics 导出 topic 到 JSON 文件
func exportTopics(broker, out string, excludeInternal bool) error {
	admin, err := newAdmin(broker)
	if err != nil {
		return err
	}
	defer admin.Close()

	topics, err := admin.ListTopics()
	if err != nil {
		return err
	}

	var result []Topic
	for name, detail := range topics {
		if excludeInternal && len(name) >= 2 && name[:2] == "__" {
			continue
		}

		// map[string]*string -> map[string]string
		configs := make(map[string]string)
		for k, v := range detail.ConfigEntries {
			if v != nil {
				configs[k] = *v
			} else {
				configs[k] = ""
			}
		}

		result = append(result, Topic{
			Name:              name,
			Partitions:        detail.NumPartitions,
			ReplicationFactor: detail.ReplicationFactor,
			Configs:           configs,
		})
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Name < result[j].Name
	})

	file := ExportFile{
		KafkaVersion: "2.4.0",
		ExportTime:   time.Now().Format(time.RFC3339),
		Topics:       result,
	}

	data, _ := json.MarshalIndent(file, "", "  ")
	return os.WriteFile(out, data, 0644)
}

// importTopics 从 JSON 文件导入 topic
func importTopics(broker, in string, ifNotExists bool) error {
	admin, err := newAdmin(broker)
	if err != nil {
		return err
	}
	defer admin.Close()

	data, err := os.ReadFile(in)
	if err != nil {
		return err
	}

	var file ExportFile
	if err := json.Unmarshal(data, &file); err != nil {
		return err
	}

	for _, t := range file.Topics {
		// map[string]string -> map[string]*string
		cfg := make(map[string]*string)
		for k, v := range t.Configs {
			vCopy := v // 避免取地址错误
			cfg[k] = &vCopy
		}

		detail := &sarama.TopicDetail{
			NumPartitions:     t.Partitions,
			ReplicationFactor: t.ReplicationFactor,
			ConfigEntries:     cfg,
		}

		err := admin.CreateTopic(t.Name, detail, false)
		if err != nil {
			if ifNotExists {
				fmt.Printf("⚠️  跳过已存在 topic: %s\n", t.Name)
				continue
			}
			return err
		}

		fmt.Printf("✅ 创建 topic: %s\n", t.Name)
	}

	return nil
}

// main 入口
func main() {
	if len(os.Args) < 2 {
		fmt.Println("用法: kafka-topicctl <export|import> [参数]")
		fmt.Println("示例:")
		fmt.Println("  kafka-topicctl export --bootstrap broker:9092")
		fmt.Println("  kafka-topicctl import --bootstrap broker:9092 --in topics.json")
		os.Exit(1)
	}

	switch os.Args[1] {

	case "export":
		fs := flag.NewFlagSet("export", flag.ExitOnError)
		broker := fs.String("bootstrap", "", "Kafka bootstrap server")
		out := fs.String("out", "topics.json", "输出文件（默认当前目录 topics.json）")
		exclude := fs.Bool("exclude-internal", true, "排除内部 topic（默认 true）")
		fs.Parse(os.Args[2:])

		if *broker == "" {
			fs.Usage()
			os.Exit(1)
		}

		if err := exportTopics(*broker, *out, *exclude); err != nil {
			panic(err)
		}

		fmt.Println("🎉 导出完成:", *out)

	case "import":
		fs := flag.NewFlagSet("import", flag.ExitOnError)
		broker := fs.String("bootstrap", "", "Kafka bootstrap server")
		in := fs.String("in", "topics.json", "导入文件（默认当前目录 topics.json）")
		ifNotExists := fs.Bool("if-not-exists", true, "存在则跳过（默认 true）")
		fs.Parse(os.Args[2:])

		if *broker == "" {
			fs.Usage()
			os.Exit(1)
		}

		if err := importTopics(*broker, *in, *ifNotExists); err != nil {
			panic(err)
		}

		fmt.Println("🎉 导入完成")

	default:
		fmt.Println("支持命令: export / import")
	}
}
