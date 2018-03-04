---
- include: ../util/set_ansible_ssh_host.yml

- hosts: all
  gather_facts: False
  tasks: 
    - name: execute remote script  
      shell: "{{ script }}"  
